import os
from pathlib import Path
import re

from loguru import logger
import polars as pl
import typer

from src.config import INTERIM_DATA_DIR

app = typer.Typer()


query = """
    WITH ranked_stats AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY scraped_at ASC) as entry_rank,
            ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY scraped_at DESC) as exit_rank
        FROM skz_stats
        WHERE
            scraped_at >= '2026-04-27 00:00:00' AND
            scraped_at < '2026-05-27 00:00:00'
    )

    SELECT
        ranked_stats.video_id,
        skz_snippets.title,
        skz_snippets.video_format,
        skz_snippets.published_at,
        MIN(ranked_stats.scraped_at) FILTER (WHERE entry_rank = 1) AS first_scraped_at,
        MAX(ranked_stats.scraped_at) FILTER (WHERE exit_rank = 1) AS last_scraped_at,
        MAX(like_count) FILTER (WHERE entry_rank = 1) AS earliest_like_count,
        MAX(like_count) FILTER (WHERE exit_rank = 1) AS latest_like_count,
        MAX(comment_count) FILTER (WHERE entry_rank = 1) AS earliest_comment_count,
        MAX(comment_count) FILTER (WHERE exit_rank = 1) AS latest_comment_count,
        MAX(view_count) FILTER (WHERE entry_rank = 1) AS earliest_view_count,
        MAX(view_count) FILTER (WHERE exit_rank = 1) AS latest_view_count
        
    FROM ranked_stats
    LEFT JOIN skz_snippets
        ON skz_snippets.video_id = ranked_stats.video_id
    GROUP BY
        ranked_stats.video_id,
        skz_snippets.title,
        skz_snippets.video_format,
        skz_snippets.published_at
    ORDER BY skz_snippets.published_at
    """


# This generates a new column to categorize contents
get_long_form_cat = (
    pl.when(pl.col("title").str.contains(r"(?i)SKZ-TALKER GO!"))
    .then(pl.lit("SKZ-TALKER GO!"))
    .when(pl.col("title").str.contains(r"(?i)SKZ-TALKER|슼토커|슼즈토커"))
    .then(pl.lit("SKZ-TALKER"))
    .when(pl.col("title").str.contains(r"\[SKZ CODE\]"))
    .then(pl.lit("SKZ CODE"))
    .when(pl.col("title").str.contains(r"(?i)Kids?'?s?\s*(Room|Song|Show)"))
    .then(pl.lit("Kids Room Series"))
    .when(pl.col("title").str.contains(r"\[SKZ VLOG\]|\[RACHA LOG\]|\[SKZ LOG\]"))
    .then(pl.lit("Vlogs & Logs"))
    .when(pl.col("title").str.contains(r"(?i)SKZ-RECORD|SKZ-PLAYER|\[SONG by\]"))
    .then(pl.lit("SKZ-RECORD / PLAYER"))
    .when(pl.col("title").str.contains(r"(?i)제 9구역|The 9th"))
    .then(pl.lit("The 9th"))
    .when(pl.col("title").str.contains(r"\[SPOT KIDS"))
    .then(pl.lit("SPOT KIDS"))
    .when(pl.col("title").str.contains(r"(?i)M/V Reaction"))
    .then(pl.lit("M/V Reaction"))
    .when(pl.col("title").str.contains(r"(?i)MAKING FILM"))
    .then(pl.lit("Making Film"))
    .when(
        pl.col("title").str.contains(
            r"(?i)Dance Practice|Performance Video|Guide Video|Lyric Visualizer"
        )
    )
    .then(pl.lit("Dance & Performance"))
    .otherwise(pl.lit("Other/Music Videos"))
)


def categorize_long_forms(title):
    if re.search(r"(?i)SKZ-TALKER GO!", title):
        return "SKZ-TALKER GO!"
    elif re.search(r"(?i)SKZ-TALKER|슼토커|슼즈토커", title):
        return "SKZ-TALKER"
    elif re.search(r"\[SKZ CODE\]", title):
        return "SKZ CODE"
    elif re.search(r"(?i)Kids?'?s?\s*(Room|Song|Show)", title):
        return "Kids Room Series"
    elif re.search(r"\[SKZ VLOG\]|\[RACHA LOG\]|\[SKZ LOG\]", title):
        return "Vlogs & Logs"
    elif re.search(r"(?i)SKZ-RECORD|SKZ-PLAYER|\[SONG by\]", title):
        return "SKZ-RECORD / PLAYER"
    elif re.search(r"(?i)제 9구역|The 9th", title):
        return "The 9th"
    elif re.search(r"\[SPOT KIDS", title):
        return "SPOT KIDS"
    elif re.search(r"(?i)M/V Reaction", title):
        return "M/V Reaction"
    elif re.search(r"(?i)MAKING FILM", title):
        return "Making Film"
    elif re.search(r"(?i)Dance Practice|Performance Video|Guide Video|Lyric Visualizer", title):
        return "Dance & Performance"
    else:
        return "Other/Music Videos"


def categorize_shorts(title):
    if re.search(r"(?i)Challenge\s+w/|챌린지\s+w/", title):
        return "Collab Challenge"

    elif re.search(r"(?i)Behind-the-scenes", title):
        return "Behind Content"

    elif re.search(r"(?i)SKZCODE|스키즈코드", title):
        return "SKZ CODE Shorts"

    elif re.search(r"(?i)SKZ_TALKER_GO|슼즈토커고", title):
        return "SKZ-TALKER GO!"

    elif re.search(
        r"(?i)dominATE|MANIAC|FANMEETING|World Tour|STAY_in_Our_Little_House|SKZ_5CLOCK",
        title,
    ):
        return "Concert & Fanmeeting"

    elif re.search(r"(?i)SONGby|송바이|SKZ_PLAYER|슼즈플레이어|Cover by", title):
        return "Music Cover / Solo Release"

    elif re.search(r"(?i)birthday|Happy\w+Day", title):
        return "Member Birthday"

    elif re.search(
        r"(?i)WalkinOnWater|合|HOP|Youth|ULTRA|SoGood|Holdmyhand|As_we_are|HALLUCINATION",
        title,
    ):
        return "HOP Era"

    elif re.search(r"(?i)DO_IT|Do_It|신선놀음|DIVINE", title):
        return "DO IT Era"

    elif re.search(r"(?i)ATE|ChkChkBoom|ILikeIt|JJAM|MOUNTAINS", title):
        return "ATE Era"

    elif re.search(r"(?i)LoseMyBreath|LMB_Challenge|숨멎챌", title):
        return "Lose My Breath Era"

    elif re.search(r"(?i)樂_STAR|ROCK_STAR|락|樂|LALALALA", title):
        return "ROCK-STAR Era"

    elif re.search(r"(?i)5_STAR|특|S_Class", title):
        return "5-STAR Era"

    elif re.search(r"(?i)CASE143|CASE143Challenge", title):
        return "Case 143 Era"

    elif re.search(r"(?i)치즈챌린지|CHEESEchallenge|소리꾼챌린지|ThunderousChallenge", title):
        return "NOEASY Era"

    else:
        return "General Promo / Other Shorts"


def categorize_lives(title):
    if re.search(r"찬이의.*방", title):
        return "Chan's Room"

    elif re.search(r"리노리방", title):
        return "Lee Know's Ri-Bang"

    elif re.search(r"(?i)생일|탄신일|탄생|데이|day|anniversary|birthday", title):
        return "Celebrations & Birthdays"

    elif re.search(
        r"(?i)countdown|unveil|라이브\s*스트림|팬미팅|콘서트|리허설|가요대전|언박싱",
        title,
    ):
        return "Official Promos & Events"

    else:
        return "Casual & Unit Lives"


get_content_pillar = (
    pl.when(pl.col("video_format") == "Long-form")
    .then(pl.col("title").map_elements(categorize_long_forms))
    .when(pl.col("video_format") == "Short")
    .then(pl.col("title").map_elements(categorize_shorts))
    .otherwise(pl.col("title").map_elements(categorize_lives))
    .cast(pl.Categorical)
)


get_video_age_days = (pl.col("last_scraped_at") - pl.col("published_at")).dt.total_days()

enum_video_age_days = [
    "New Release (<30 Days)",
    "Recent (1-6 Months)",
    "Catalog (6-24 Months)",
    "Legacy (2+ Years)",
]


get_video_age_cohort = (
    pl.when(get_video_age_days < 30)
    .then(pl.lit("New Release (<30 Days)"))
    .when(get_video_age_days < 180)
    .then(pl.lit("Recent (1-6 Months)"))
    .when(get_video_age_days < 720)
    .then(pl.lit("Catalog (6-24 Months)"))
    .otherwise(pl.lit("Legacy (2+ Years)"))
    .cast(pl.Enum(enum_video_age_days))
)


get_lifetime_engagement_rate = (
    pl.when(pl.col("latest_view_count") == 0)
    .then(None)
    .otherwise(
        (pl.col("latest_like_count") + pl.col("latest_comment_count"))
        / pl.col("latest_view_count")
    )
)


get_marginal_ratio = (
    pl.when(pl.col("latest_view_count") - pl.col("earliest_view_count") == 0)
    .then(None)
    .otherwise(
        (pl.col("latest_like_count") - pl.col("earliest_like_count"))
        / (pl.col("latest_view_count") - pl.col("earliest_view_count"))
    )
)


days_elapsed = (pl.col("last_scraped_at") - pl.col("first_scraped_at")) / pl.duration(days=1)

get_daily_view_velocity = (
    pl.when(days_elapsed == 0)
    .then(None)
    .otherwise((pl.col("latest_view_count") - pl.col("earliest_view_count")) / days_elapsed)
)


@app.command()
def make_data(
    db_uri_key: str = typer.Option(
        "DB_URI_KEY", help="URI key of the database containing your data."
    ),
    sentiment_result_path: Path = INTERIM_DATA_DIR / "df_sentiment_final_result.parquet",
    output_path: Path = INTERIM_DATA_DIR / "dataset.parquet",
    return_data: bool = False,
):
    # Get the sentiment analysis result
    if sentiment_result_path.exists():
        df_sentiment_result = pl.scan_parquet(sentiment_result_path)
        logger.success("Dataset containing sentiment analysis result downloaded successfully.")
    else:
        # Perform sentiment analysis if not
        from src.analyses.sentiment import perform_sentiment_analysis

        logger.warning("Sentiment result not found in the directory.")
        logger.info("Starting to perform sentiment analysis")
        try:
            df_sentiment_result = perform_sentiment_analysis(
                db_uri_key=db_uri_key, return_data=True
            )
            if df_sentiment_result is None:
                logger.error("Sentiment analysis returned no result.")
                return
            if isinstance(df_sentiment_result, pl.DataFrame):
                df_sentiment_result = df_sentiment_result.lazy()

            logger.success("Sentiment analysis performed successfully")
        except Exception as e:
            logger.error(f"Sentiment analysis incomplete: {e}")
            return

    db_uri = os.environ.get(db_uri_key, "")
    if not db_uri:
        logger.error("Database URI not found.")
        return

    logger.info("Fetching data from database...")
    try:
        df_lazy = pl.read_database_uri(query, db_uri).lazy()
        logger.success("Data fetched from database successfully.")
    except Exception as e:
        logger.error(f"Failed to fetch data from database: {e}")
        return

    logger.info("Starting to engineer main features...")

    # Enumerate video formats
    enum_formats = ["Live/VOD", "Long-form", "Short"]

    df_engineered = (
        df_lazy.select(
            pl.col("video_id", "title"),
            # Convert the data type of `video_format` to Enum
            pl.col("video_format").cast(pl.Enum(enum_formats)),
            pl.col(pl.Datetime),
            pl.col(pl.Int64),
        )
        .with_columns(
            content_pillar=get_content_pillar,
            publish_year=pl.col("published_at").dt.year().cast(pl.UInt16),
            publish_month=pl.col("published_at").dt.month().cast(pl.UInt8),
            video_age_cohort=get_video_age_cohort,
            video_age_days=get_video_age_days.cast(pl.UInt16),
            lifetime_engagement_rate=get_lifetime_engagement_rate,
            marginal_ratio=get_marginal_ratio,
            daily_view_velocity=get_daily_view_velocity,
        )
        .select(pl.all().exclude(pl.Datetime, pl.Int64))
        .join(df_sentiment_result, on="video_id", how="left")
    )

    logger.success("Main features engineered successfully.")

    if return_data:
        return df_engineered
    else:
        df_engineered.sink_parquet(output_path)
        logger.success("Dataset loaded successfully.")


if __name__ == "__main__":
    app()
