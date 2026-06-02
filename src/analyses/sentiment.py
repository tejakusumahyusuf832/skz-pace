import os

import fasttext
from huggingface_hub import hf_hub_download, login
from loguru import logger
import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

from src.config import MODELS_DIR

query = """
    SELECT DISTINCT ON (comment_id) 
        video_id, 
        comment_id, 
        text
    FROM skz_top_comments
    WHERE
        scraped_at >= '2026-04-27 00:00:00' AND
        scraped_at < '2026-05-27 00:00:00'
    ORDER BY
        comment_id,
        scraped_at DESC
"""

# === FUNCTION ===
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


def batch_detect_language(text_series: pl.Series) -> pl.Series:
    text_list = text_series.to_list()
    labels, _ = lang_model.predict(text_list, k=1)
    cleaned_labels = [lbl[0].replace("__label__", "") for lbl in labels]
    return pl.Series(cleaned_labels)


# ----------------

# === FUNCTION ===
model_id = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
local_dir = MODELS_DIR / "hugging-face"

try:
    tokenizer = AutoTokenizer.from_pretrained(local_dir, local_files_only=True)
    model = AutoModelForSequenceClassification.from_pretrained(local_dir, local_files_only=True)
except Exception:
    logger.warning("Model missing or incomplete locally. Downloading from Hugging Face...")

    tokenizer = AutoTokenizer.from_pretrained(model_id)
    model = AutoModelForSequenceClassification.from_pretrained(model_id)
    tokenizer.save_pretrained(local_dir)
    model.save_pretrained(local_dir)
    logger.success("Download complete and safely stored!")


sentiment_pipeline = pipeline(
    "sentiment-analysis",  # type: ignore
    model=model,
    tokenizer=tokenizer,
    truncation=True,
    max_length=512,
)


def perform_sentiment_analysis(text_series: pl.Series) -> pl.Series:
    text_list = text_series.to_list()
    results = sentiment_pipeline(text_list)
    sentiment_labels = [result["label"] for result in results]
    return pl.Series(sentiment_labels)


# ----------------

db_uri_key = "DB_URI_KEY"

DB_URI = os.environ.get(db_uri_key, "")
if not DB_URI:
    logger.error("Database URI not found.")

logger.info("Fetching comment data from database...")
try:
    df_lazy = pl.read_database_uri(query, DB_URI).lazy()
    logger.success("Comment data fetched successfully.")
except Exception as e:
    logger.error(f"Failed to fetch comment data from database: {e}")

logger.info("Starting to detect languages...")


df_lang_labeled = df_lazy.with_columns(
    pl.col("text")
    .fill_null("")
    .str.replace_all("\n", " ", literal=True)
    .map_batches(batch_detect_language, return_dtype=pl.String)
    .alias("language")
)


df_selected_lang = (
    df_lang_labeled.select(pl.col("language").value_counts(sort=True))
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

chosen_langs = hf_langs | set(selected_lang_list)
chosen_langs_list = list(chosen_langs)

df_selected_labels = df_lang_labeled.filter(pl.col("language").is_in(chosen_langs_list))

df_selected_labels.head(8).collect()

df_sentiment_labels = df_selected_labels.with_columns(
    pl.col("text")
    .map_batches(perform_sentiment_analysis, return_dtype=pl.String)
    .alias("sentiment_label")
)

df_sentiment_labels_eager = df_sentiment_labels.collect()
