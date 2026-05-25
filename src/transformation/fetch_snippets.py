"""Transform and load raw YouTube snippet payloads into structured databases.

Pulls unprocessed JSON blobs from the cloud data lake, flattens the metadata
into tabular records, and performs upserts into the transformed schema.
"""

from enum import Enum
import os
from typing import Any, Sequence

from loguru import logger
from sqlalchemy import text
import typer

from src.load.db.connection import is_connected_to_db

app = typer.Typer()


class StorageOptions(str, Enum):
    DATABASE = "DATABASE"
    GDRIVE = "GDRIVE"


def get_new_snippets(
    storage_mode,
    last_scraped_at,
    *,
    engine: Any = None,
    service: Any = None,
    folder_id: str = "FOLDER_ID",
) -> tuple:
    if storage_mode == "DATABASE":
        if engine is None:
            raise ValueError("engine is required when storage_mode is 'DATABASE'")

        formats_map = {}
        with engine.connect() as conn:
            result = conn.execute(text("SELECT video_id, video_format FROM processed_vids"))
            for row in result:
                formats_map[row[0]] = row[1]

        query = "SELECT scraped_at, video_response FROM snippets_and_stats"
        if last_scraped_at:
            query += f" WHERE scraped_at > '{last_scraped_at}'"
        query += " ORDER BY scraped_at ASC"

        with engine.connect() as conn:
            logger.info("Fetching new snippet data from raw database...")
            raw_results = conn.execute(text(query)).mappings().all()

    else:
        if service is None or folder_id is None:
            raise ValueError("service and folder_id are required when storage_mode is 'GDRIVE'")

        from src.load.gdrive.file_management import load_jsonl_file

        format_map_list = load_jsonl_file(
            service,
            filename="processed_vids.jsonl",
            folder_id=folder_id,
            desired_keys=["video_id", "video_format"],
        )

        formats_map = {row.get("video_id"): row.get("video_format") for row in format_map_list}

        logger.info("Fetching new snippets data from raw Drive...")
        raw_results = load_jsonl_file(
            service,
            filename="snippets_and_stats.jsonl",
            folder_id=folder_id,
            desired_keys=["scraped_at", "video_response"],
            filter_date_scraped=last_scraped_at,
        )

    return raw_results, formats_map


def upsert_snippets(engine: Any, data_list: list) -> None:
    """Insert new video snippets or update existing ones on primary key conflict.

    Args:
        data_list (list): A list of dictionaries containing flattened snippet data.
        db_uri (str): The target database connection string.
    """
    if not data_list:
        logger.info("No snippet records to upsert. Skipping DB load.")
        return

    query = text("""
        INSERT INTO skz_snippets 
        (video_id, published_at, video_format, title, description, category_id, tags, video_link, scraped_at)
        VALUES 
        (:video_id, :published_at, :video_format, :title, :description, :category_id, :tags, :video_link, :scraped_at)
        ON CONFLICT (video_id) 
        DO UPDATE SET 
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            category_id = EXCLUDED.category_id,
            tags = EXCLUDED.tags,
            scraped_at = EXCLUDED.scraped_at;
    """)

    try:
        with engine.begin() as conn:
            conn.execute(query, data_list)
        logger.success(f"Successfully upserted {len(data_list)} records to 'skz_snippets'")
    except Exception as e:
        logger.error(f"Database upsert failed for skz_snippets: {e}")


def process_snippets(raw_data: Sequence[Any], formats_map: dict) -> list:
    """Flatten raw API JSON payloads into standardized dictionary records.

    Args:
        raw_data (Sequence[Any]): Database row proxy objects containing the raw JSON.
        formats_map (dict): A mapping of video IDs to their respective formats.

    Returns:
        list: A list of dictionaries matching the 'skz_snippets' table schema.
    """
    snippet_records = []

    for row in raw_data:
        vid_response = row["video_response"]
        scraped_at = row["scraped_at"]

        for vid_info in vid_response.get("items", []):
            video_id = vid_info["id"]
            snippet = vid_info["snippet"]

            snippet_records.append(
                {
                    "video_id": video_id,
                    "published_at": snippet.get("publishedAt"),
                    "video_format": formats_map.get(video_id, "Unknown"),
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "category_id": snippet.get("categoryId"),
                    "tags": ",".join(snippet.get("tags", [])),
                    "video_link": f"https://www.youtube.com/watch?v={video_id}",
                    "scraped_at": scraped_at,
                }
            )

    return snippet_records


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
            result = conn.execute(text("SELECT MAX(scraped_at) FROM skz_snippets"))
            last_scraped_at = result.scalar()
    except Exception as e:
        logger.warning(f"Could not read from local DB (table might be empty): {e}")
        last_scraped_at = None

    # Get raw, new snippets
    if storage_mode_start == "DATABASE":
        raw_results, formats_map = get_new_snippets(
            "DATABASE", last_scraped_at, engine=engine_start
        )
    else:
        raw_results, formats_map = get_new_snippets(
            "GDRIVE", last_scraped_at, service=drive_service, folder_id=drive_folder_id
        )

    if not raw_results:
        logger.info("No new snippets to process.")
        return

    # Transform and Upsert
    logger.info(f"Transforming {len(raw_results)} new batches...")
    transformed_data = process_snippets(raw_results, formats_map)
    upsert_snippets(engine_end, transformed_data)


if __name__ == "__main__":
    app()
