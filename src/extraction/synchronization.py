from datetime import datetime, timezone
import os
from typing import Any

from loguru import logger


def prepare_authentication(
    storage_mode: str = "gdrive",
    *,
    db_uri_key: str = "DB_URI",
    gcp_credentials_key: str = "GCP_SA_CREDENTIALS",
    drive_folder_id_key: str = "DRIVE_FOLDER_ID",
) -> Any:
    if storage_mode == "database":
        from src.load.db.connection import is_connected_to_db

        # Check the database connection
        db_uri_connection = is_connected_to_db(db_uri_key)
        if not db_uri_connection:
            return None

        # Get the database URL
        return os.environ.get(db_uri_key, "")

    else:
        from src.load.gdrive.authentication import get_drive_service
        from src.load.gdrive.storage import load_file

        # Initialize Drive service
        drive_service = get_drive_service(gcp_creds=gcp_credentials_key)
        folder_id = os.environ.get(drive_folder_id_key, "")
        state_filename = "processed_vids.jsonl"

        # Load processed state data from Drive
        try:
            processed_state_data = load_file(
                service=drive_service, filename=state_filename, folder_id=folder_id
            )
            return drive_service, folder_id, processed_state_data
        except Exception as e:
            logger.warning(f"Could not read state from Drive. Error: {e}")
            return drive_service, folder_id, []


def get_old_processed_ids(
    storage_mode: str = "gdrive",
    db_uri: str = "DB_URI",
    processed_state_data: list = [],
) -> list[str] | list:
    if storage_mode == "database":
        # Import database packages
        from sqlalchemy import create_engine, text

        # Fetch old processed IDs
        try:
            engine = create_engine(db_uri)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT video_id FROM processed_vids"))
                old_processed_ids = [row[0] for row in result]
            return old_processed_ids
        except Exception as e:
            logger.warning(f"Could not read from DB. Error: {e}")
            return []

    else:
        old_processed_ids = [item["video_id"] for item in processed_state_data]
        return old_processed_ids


def store_raw_metadata(
    fetched_snippets_and_stats: list[dict],
    fetched_processed_vids: list[dict],
    fetched_top_comments: list[dict],
    storage_mode: str,
    *,
    db_uri: str | None = None,
    drive_service: Any | None = None,
    folder_id: str | None = None,
):
    if storage_mode == "database":
        if db_uri is None:
            raise ValueError("db_uri is required when storage_mode is 'database'")

        from src.load.db.storage import append_to_db, prune_old_raw_data

        logger.info("Routing batch results directly to database...")
        append_to_db(fetched_snippets_and_stats, "snippets_and_stats", db_uri)
        append_to_db(fetched_processed_vids, "processed_vids", db_uri)
        append_to_db(fetched_top_comments, "top_comments", db_uri)

        logger.info("Cleaning up old raw data to save cloud storage space...")
        prune_old_raw_data(db_uri, days_old=7)

    elif storage_mode == "gdrive":
        if drive_service is None or folder_id is None:
            raise ValueError(
                "drive_service and folder_id are required when storage_mode is 'gdrive'"
            )

        from src.load.gdrive.storage import save_to_drive_jsonl

        logger.info("Routing batch results to Google Drive...")

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        save_to_drive_jsonl(
            drive_service,
            fetched_snippets_and_stats,
            "processed_vids.jsonl",
            folder_id,
        )
        save_to_drive_jsonl(
            drive_service,
            fetched_snippets_and_stats,
            f"snippets_and_stats_{date_str}.jsonl",
            folder_id,
        )
        save_to_drive_jsonl(
            drive_service,
            fetched_snippets_and_stats,
            f"top_comments_{date_str}.jsonl",
            folder_id,
        )

    else:
        raise ValueError(f"Unsupported storage_mode: {storage_mode}")
