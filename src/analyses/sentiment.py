"""Perform language detection and sentiment analysis on YouTube top comments."""

import os
from pathlib import Path

import fasttext
from huggingface_hub import hf_hub_download, login
from loguru import logger
import polars as pl
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
import typer

from src.config import INTERIM_DATA_DIR, MODELS_DIR

app = typer.Typer()

query = """
    SELECT DISTINCT ON (comment_id) 
        video_id, 
        comment_id, 
        text
    FROM skz_top_comments
    WHERE
        scraped_at >= '2026-05-17 00:00:00' AND
        scraped_at < '2026-06-16 00:00:00'
    ORDER BY
        comment_id,
        scraped_at DESC
"""


def detect_language(text_series: pl.Series) -> pl.Series:
    """Detect the primary language of each text entry in a series.

    Args:
        text_series (pl.Series): A Polars Series containing the string text to analyze.

    Returns:
        pl.Series: A Polars Series containing the detected language codes.
    """
    HF_TOKEN = os.environ.get("HF_TOKEN", "")
    if not HF_TOKEN:
        logger.error("HF_TOKEN not found.")

    login(token=HF_TOKEN)

    model_path = hf_hub_download(
        repo_id="facebook/fasttext-language-identification",
        filename="model.bin",
        local_dir=MODELS_DIR / "fasttext",
        local_files_only=True,
    )

    lang_model = fasttext.load_model(model_path)
    text_list = text_series.to_list()
    labels, _ = lang_model.predict(text_list, k=1)
    cleaned_labels = [lbl[0].replace("__label__", "") for lbl in labels]
    return pl.Series(cleaned_labels)


def get_sentiment_labels(text_series: pl.Series) -> pl.Series:
    """Apply a pre-trained sequence classification model to predict sentiment labels.

    Args:
        text_series (pl.Series): A Polars Series containing the string text to analyze.

    Returns:
        pl.Series: A Polars Series containing the predicted sentiment string labels.
    """
    MODEL_ID = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
    MODEL_PATH = MODELS_DIR / "best-kpop-sentiment-model"

    try:
        if not MODEL_PATH.exists():
            MODEL_PATH = MODELS_DIR / "hugging-face"

        logger.info(f"Loading {MODEL_PATH.name} from the model directory...")
        tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, local_files_only=True)
        model = AutoModelForSequenceClassification.from_pretrained(
            MODEL_PATH, local_files_only=True
        )
        logger.success("Model loaded successfully.")

    except Exception:
        logger.warning("Model missing or incomplete locally. Downloading from Hugging Face...")

        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID)
        tokenizer.save_pretrained(MODEL_PATH)
        model.save_pretrained(MODEL_PATH)
        logger.success("Download complete and safely stored!")

    sentiment_pipeline = pipeline(
        "sentiment-analysis",  # type: ignore
        model=model,
        tokenizer=tokenizer,
        truncation=True,
        max_length=512,
        device=0,
        dtype=torch.float16,
    )

    text_list = [str(t) if t is not None else "" for t in text_series.to_list()]
    results = sentiment_pipeline(text_list, batch_size=64)
    sentiment_labels = [result["label"] for result in results]
    return pl.Series(sentiment_labels)


@app.command()
def perform_sentiment_analysis(
    db_uri_key: str = typer.Option(
        "DB_URI_KEY", help="URI key of the database containing your data."
    ),
    output_path: Path = INTERIM_DATA_DIR / "video_sentiment_result.parquet",
    return_data: bool = False,
) -> pl.LazyFrame | None:
    """Execute the end-to-end sentiment analysis pipeline via the command line interface.

    Args:
        db_uri_key (str, optional): The environment variable key mapped to the database connection URI.
        output_path (Path, optional): The file path where the resulting parquet file should be saved.
        return_data (bool, optional): Determine whether to return the DataFrame directly
            instead of writing it to disk. Defaults to False.

    Returns:
        pl.LazyFrame | None: The resulting sentiment LazyFrame if return_data is True, otherwise None.
    """
    DB_URI = os.environ.get(db_uri_key, "")
    if not DB_URI:
        logger.error("Database URI not found.")

    logger.info("Fetching comment data from database...")
    try:
        lazy_df = pl.read_database_uri(query, DB_URI).lazy()
        lazy_df.sink_parquet(INTERIM_DATA_DIR / "top_comments_30_days.parquet")
        logger.success("Comment data fetched successfully.")
    except Exception as e:
        logger.error(f"Failed to fetch comment data from database: {e}")
        return

    logger.info("Starting to detect languages...")

    # Label every comment with its language
    lang_detected_df = lazy_df.with_columns(
        pl.col("text")
        .fill_null("")
        .str.replace_all("\n", " ", literal=True)
        .map_batches(detect_language, return_dtype=pl.String)
        .alias("language")
    )

    lang_detected_df.sink_parquet(INTERIM_DATA_DIR / "comment_lang_detected.parquet")

    # Select only the languages that mainly appear 90% of the data
    df_selected_lang = (
        lang_detected_df.select(pl.col("language").value_counts(sort=True))
        .unnest("language")
        .with_columns((pl.col("count") * 100 / pl.col("count").sum()).alias("lang_percentage"))
        .with_columns(pl.col("lang_percentage").cum_sum().alias("cum_sum"))
        .filter(pl.col("cum_sum") <= 90)
    )

    selected_lang_list = df_selected_lang.collect().get_column("language").to_list()

    hf_langs = {
        "arb_Arab",
        "eng_Latn",
        "fra_Latn",
        "deu_Latn",
        "hin_Deva",
        "ita_Latn",
        "spa_Latn",
        "por_Latn",
    }

    # Combine the selected languages with the languages supported by Hugging Face
    chosen_langs = hf_langs | set(selected_lang_list)
    chosen_langs_list = list(chosen_langs)

    df_selected_labels = lang_detected_df.filter(pl.col("language").is_in(chosen_langs_list))

    comment_sentiment_df = df_selected_labels.with_columns(
        pl.col("text")
        .map_batches(get_sentiment_labels, return_dtype=pl.String)
        .alias("sentiment_label")
    )

    comment_sentiment_df.sink_parquet(INTERIM_DATA_DIR / "comment_sentiment.parquet")

    video_sentiment_result_lf = (
        comment_sentiment_df.select(
            pl.col("video_id", "comment_id"),
            pl.when(pl.col("sentiment_label") == "positive")
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("is_positive"),
        )
        .group_by(pl.col("video_id"))
        .agg(pl.col("is_positive").mean().alias("positive_sentiment_percentage"))
        .with_columns(pl.col("positive_sentiment_percentage").mul(100))
    )

    if return_data:
        return video_sentiment_result_lf
    else:
        video_sentiment_result_lf.sink_parquet(output_path)


if __name__ == "__main__":
    app()
