"""Transform and load raw YouTube performance statistics into structured databases.

Extracts batched JSON responses from the cloud raw data lake, isolates daily
time-series statistics (views, likes, comments), and writes them to the local schema.
"""

from enum import Enum
import os
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


def get_new_stats(
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

        query = "SELECT scraped_at, video_response FROM snippets_and_stats"
        if last_scraped_at:
            query += f" WHERE scraped_at > '{last_scraped_at}'"
        query += " ORDER BY scraped_at ASC"

        with engine.connect() as conn:
            logger.info("Fetching new stats data from raw database...")
            raw_results = conn.execute(text(query)).mappings().all()

    else:
        if service is None or folder_id is None:
            raise ValueError("service and folder_id are required when storage_mode is 'GDRIVE'")

        from src.load.gdrive.file_management import load_jsonl_file

        logger.info("Fetching new stats data from raw Drive...")
        raw_results = load_jsonl_file(
            service,
            filename="snippets_and_stats.jsonl",
            folder_id=folder_id,
            desired_keys=["scraped_at", "video_response"],
            filter_date_scraped=last_scraped_at,
        )

    return raw_results


def process_stats(raw_data: Sequence[Any]) -> list:
    """Parse raw API statistics payloads into standardized flat dictionaries.

    Args:
        raw_data (Sequence[Any]): Database mapping proxy containing nested API JSON.

    Returns:
        list: A list of flattened statistic records ready for database insertion.
    """
    stats_records = []

    for row in raw_data:
        vid_response = row["video_response"]
        scraped_at = row["scraped_at"]

        for vid_info in vid_response.get("items", []):
            video_id = vid_info["id"]
            stats = vid_info.get("statistics", {})

            stats_records.append(
                {
                    "video_id": video_id,
                    "scraped_at": scraped_at,
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                }
            )

    return stats_records


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
            result = conn.execute(text("SELECT MAX(scraped_at) FROM skz_stats"))
            last_scraped_at = result.scalar()
    except Exception as e:
        logger.warning(f"Could not read from local DB (table might be empty): {e}")
        last_scraped_at = None

    # Get raw, new snippets
    if storage_mode_start == "DATABASE":
        raw_results = get_new_stats("DATABASE", last_scraped_at, engine=engine_start)
    else:
        raw_results = get_new_stats(
            "GDRIVE", last_scraped_at, service=drive_service, folder_id=drive_folder_id
        )

    if not raw_results:
        logger.info("No new stats to process.")
        return

    transformed_data = process_stats(raw_results)
    append_to_db(transformed_data, "skz_stats", engine_end)


if __name__ == "__main__":
    app()
