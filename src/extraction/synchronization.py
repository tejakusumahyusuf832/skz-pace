import os
from typing import Any

from loguru import logger


def prepare_authentication(
    storage_mode: str = "GDRIVE",
    *,
    db_uri_key: str = "DB_URI",
    gcp_credentials_key: str = "GCP_CREDENTIALS",
    drive_folder_id_key: str = "DRIVE_FOLDER_ID",
) -> Any:
    if storage_mode == "DATABASE":
        # --- LOCAL IMPORTS ---
        from src.load.db.connection import is_connected_to_db

        # Check the database connection
        db_status, db_engine = is_connected_to_db(db_uri_key)
        return db_engine if db_status else None

    else:
        # --- LOCAL IMPORTS ---
        from src.load.gdrive.authentication import get_drive_service

        # Initialize Drive service
        drive_service = get_drive_service(gcp_credentials_key)
        folder_id = os.environ.get(drive_folder_id_key, "")
        return drive_service, folder_id


def get_old_processed_ids(
    storage_mode: str = "GDRIVE",
    *,
    db_engine: Any = None,
    drive_service: Any = None,
    folder_id: str | None = None,
) -> list[str] | list:
    if storage_mode == "DATABASE":
        if db_engine is None:
            raise ValueError("db_engine is required when storage_mode is 'DATABASE'")

        # --- LOCAL IMPORTS ---
        from sqlalchemy import text

        try:
            with db_engine.connect() as conn:
                result = conn.execute(text("SELECT video_id FROM processed_vids"))
                old_processed_ids = [row[0] for row in result]
            return old_processed_ids
        except Exception as e:
            logger.warning(f"Could not read from DB. Error: {e}")
            return []

    else:
        if drive_service is None or folder_id is None:
            raise ValueError(
                "drive_service and folder_id are required when storage_mode is 'GDRIVE'"
            )

        # # --- LOCAL IMPORTS ---
        from src.load.gdrive.file_management import load_jsonl_file

        old_processed_ids = load_jsonl_file(
            drive_service,
            filename="processed_vids.jsonl",
            folder_id=folder_id,
            desired_keys="video_id",
        )
        return old_processed_ids


def store_raw_metadata(
    fetched_snippets_and_stats: list[dict],
    fetched_processed_vids: list[dict],
    fetched_top_comments: list[dict],
    storage_mode: str,
    *,
    db_engine: Any = None,
    drive_service: Any = None,
    folder_id: str | None = None,
):
    if storage_mode == "DATABASE":
        if db_engine is None:
            raise ValueError("db_engine is required when storage_mode is 'DATABASE'")

        # --- LOCAL IMPORTS ---
        from src.load.db.storage import append_to_db, prune_old_raw_data

        logger.info("Routing batch results directly to database...")
        append_to_db(fetched_snippets_and_stats, "snippets_and_stats", db_engine)
        append_to_db(fetched_processed_vids, "processed_vids", db_engine)
        append_to_db(fetched_top_comments, "top_comments", db_engine)

        logger.info("Cleaning up old raw data to save cloud storage space...")
        prune_old_raw_data(db_engine, days_old=7)

    elif storage_mode == "GDRIVE":
        if drive_service is None or folder_id is None:
            raise ValueError(
                "drive_service and folder_id are required when storage_mode is 'GDRIVE'"
            )

        # --- LOCAL IMPORTS ---
        from src.load.gdrive.file_management import update_to_drive_jsonl

        logger.info("Routing batch results to Google Drive...")

        update_to_drive_jsonl(
            drive_service,
            folder_id,
            new_data=fetched_processed_vids,
            filename="processed_vids.jsonl",
        )
        update_to_drive_jsonl(
            drive_service,
            folder_id,
            new_data=fetched_snippets_and_stats,
            filename="snippets_and_stats.jsonl",
        )
        update_to_drive_jsonl(
            drive_service,
            folder_id,
            new_data=fetched_top_comments,
            filename="top_comments.jsonl",
        )

    else:
        raise ValueError(f"Unsupported storage_mode: {storage_mode}")
