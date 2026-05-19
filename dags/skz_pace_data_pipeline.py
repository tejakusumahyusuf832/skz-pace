from datetime import timedelta

from airflow.sdk import dag, task  # type: ignore
import pendulum


@dag(
    dag_id="skz_pace_daily_pipeline",
    schedule=None,
    # schedule="45 1 * * *",  # Runs every day at 1:45 UTC
    start_date=pendulum.datetime(2026, 5, 6, tz="UTC"),
    # catchup=True,
    catchup=False,
    dagrun_timeout=timedelta(hours=12),
    tags=["skz_pace", "etl", "daily"],
)
def skz_pace_pipeline():
    """
    Orchestrates the daily ETL pipeline for the SKZ Pace project.

    This DAG manages the extraction, transformation, and loading (ETL) of YouTube
    data from a raw cloud data lake into a structured local database schema. It
    is designed to process daily metadata, performance metrics, audience engagement,
    and video transcripts for downstream analytics and NLP tasks.

    The pipeline consists of four independent, parallel tasks:
        * process_snippets: Flattens and upserts JSON video metadata payloads.
        * process_stats: Isolates and loads daily time-series statistics (views, likes).
        * process_top_comments: Formats unstructured top-level comment text and authorship.
        * process_transcripts: Retrieves and formats closed captions with built-in API rate limiting.

    DAG Configuration:
        * Schedule: Daily at 13:00 UTC
        * Catchup: True (will process historical runs starting from May 7, 2026)
        * Timeout: 12 hours
    """

    @task
    def process_snippets():
        """Transform and load raw YouTube snippet payloads into structured databases.

        Pulls unprocessed JSON blobs from the cloud data lake, flattens the metadata
        into tabular records, and performs upserts into the transformed schema.
        """
        from src.etl.fetch_snippets import main

        main(uri_key_start="RAW_SKZ_PACE_DB_URL", uri_key_end="DOCKER_TRANSFORMED_SKZ_PACE_DB_URL")

    @task
    def process_stats():
        """Transform and load raw YouTube performance statistics into structured databases.

        Extracts batched JSON responses from the cloud raw data lake, isolates daily
        time-series statistics (views, likes, comments), and writes them to the local schema.
        """
        from src.etl.fetch_stats import main

        main(uri_key_start="RAW_SKZ_PACE_DB_URL", uri_key_end="DOCKER_TRANSFORMED_SKZ_PACE_DB_URL")

    @task
    def process_top_comments():
        """Transform and load top-level YouTube comments into structured databases.

        Extracts unstructured text data from the raw data lake, isolates text bodies
        and authorship details, and formats them for downstream NLP and sentiment analysis.
        """
        from src.etl.fetch_top_comments import main

        main(uri_key_start="RAW_SKZ_PACE_DB_URL", uri_key_end="DOCKER_TRANSFORMED_SKZ_PACE_DB_URL")

    @task
    def process_transcripts():
        """Retrieve, format, and load closed caption transcripts for video content.

        Fetches specific long-form video transcripts via an undocumented YouTube API
        and incorporates sleep jitter to prevent automated connection blocking.
        """
        from src.etl.fetch_transcripts import main

        main(limit=0, uri_key="DOCKER_TRANSFORMED_SKZ_PACE_DB_URL")

    # --- ORCHESTRATION / DEPENDENCY MAPPING ---

    process_snippets() >> [process_stats(), process_top_comments()] >> process_transcripts()  # type: ignore


dag = skz_pace_pipeline()
