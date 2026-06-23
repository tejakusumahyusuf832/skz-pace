"""Transform and load top-level YouTube comments into structured databases.

Extracts unstructured text data from the raw data lake, isolates text bodies
and authorship details, and formats them for downstream NLP and sentiment analysis.
"""

from enum import Enum
import os
import time
from typing import Any, Sequence

from loguru import logger
from sqlalchemy import text
import typer

from src.load.db.connection import is_connected_to_db
from src.load.db.storage import append_to_db

app = typer.Typer()


class StorageOptions(str, Enum):
    DATABASE = "DATABASE"
    GDRIVE = "GDRIVE"


def get_new_top_comments(
    storage_mode,
    last_scraped_at,
    *,
    engine: Any = None,
    service: Any = None,
    folder_id: str = "FOLDER_ID",
):
    if storage_mode == "DATABASE":
        if engine is None:
            raise ValueError("engine is required when storage_mode is 'DATABASE'")

        query = "SELECT video_id, scraped_at, comment_response FROM top_comments"
        if last_scraped_at:
            query += f" WHERE scraped_at > '{last_scraped_at}'"
        query += " ORDER BY scraped_at ASC"

        with engine.connect() as conn:
            logger.info("Fetching new comments data from raw database...")
            raw_results = conn.execute(text(query)).mappings().all()

    else:
        if service is None or folder_id is None:
            raise ValueError("service and folder_id are required when storage_mode is 'GDRIVE'")

        from src.load.gdrive.file_management import load_jsonl_file

        logger.info("Fetching new comments data from raw database...")
        raw_results = load_jsonl_file(
            service,
            filename="top_comments.jsonl",
            folder_id=folder_id,
            desired_keys=["video_id", "scraped_at", "comment_response"],
            filter_date_scraped=last_scraped_at,
        )

    return raw_results


def process_comments(raw_data: Sequence[Any]) -> list:
    """Parse nested JSON comment threads into standardized dictionary objects.

    Args:
        raw_data (Sequence[Any]): Row proxies containing the raw comments JSON from DB.

    Returns:
        list: A flattened list of individual top-level comment details.
    """
    comment_data = []

    for row in raw_data:
        comment_response = row["comment_response"]
        scraped_at = row["scraped_at"]
        video_id = row["video_id"]

        for item in comment_response.get("items", []):
            top_comment = item["snippet"]["topLevelComment"]["snippet"]
            comment_data.append(
                {
                    "video_id": video_id,
                    "comment_id": item["id"],
                    "author": top_comment.get("authorDisplayName", "Unknown"),
                    "text": top_comment.get("textDisplay", ""),
                    "like_count": int(top_comment.get("likeCount", 0)),
                    "published_at": top_comment.get("publishedAt"),
                    "scraped_at": scraped_at,
                }
            )

    return comment_data


@app.command()
def main(
    storage_mode_start: str = typer.Option(
        StorageOptions.GDRIVE,
        help="Storage for the initial metadata storage. Either 'DATABASE' or 'GDRIVE'",
    ),
    uri_key_start: str = typer.Option("URI_KEY_START", help="DB URI key containing the raw data"),
    uri_key_end: str = typer.Option(
        "URI_KEY_END", help="DB URI key containing the transformed data"
    ),
    gcp_credentials_key: str = typer.Option(
        "GCP_CREDENTIALS", help="The .env key containing the GCP credentials"
    ),
    folder_id_key: str = typer.Option(
        "DRIVE_FOLDER_ID", help="The .env key containing the Drive folder ID"
    ),
) -> None:
    # Prepare authentication
    if storage_mode_start == "DATABASE":
        status, engine_start = is_connected_to_db(uri_key_start)
        if not status:
            return
    else:
        from src.load.gdrive.authentication import get_drive_service

        drive_folder_id = os.environ.get(folder_id_key, "")
        drive_service = get_drive_service(gcp_credentials_key)
        if not drive_folder_id or not drive_service:
            return

    # Connect to the database destination
    status, engine_end = is_connected_to_db(uri_key_end)
    if not status:
        return

    # Get the last scraped date
    try:
        with engine_end.connect() as conn:
            result = conn.execute(text("SELECT MAX(scraped_at) FROM skz_top_comments"))
            last_scraped_at = result.scalar()
    except Exception as e:
        logger.warning(f"Could not read from local DB (table might be empty): {e}")
        last_scraped_at = None

    # Get raw, new snippets
    if storage_mode_start == "DATABASE":
        raw_results = get_new_top_comments("DATABASE", last_scraped_at, engine=engine_start)
    else:
        raw_results = get_new_top_comments(
            "GDRIVE", last_scraped_at, service=drive_service, folder_id=drive_folder_id
        )

    if not raw_results:
        logger.info("No new comments to process.")
        return

    logger.info(f"Processing top comments from {storage_mode_start}...")
    transformed_data = process_comments(raw_results)
    logger.success("Top comments processed successfully.")

    logger.info("Appending new top comments to database...")
    start_time = time.perf_counter()
    append_to_db(transformed_data, "skz_top_comments", engine_end)
    end_time = time.perf_counter()
    execution_time = (end_time - start_time) / 60
    logger.info(f"Execution time of appending: {execution_time:.2f} minutes.")


if __name__ == "__main__":
    app()
