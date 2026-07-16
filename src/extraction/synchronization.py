"""Manage synchronization and routing of extraction data to the appropriate storage backend."""

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
    """Initialize and return the authentication client for the selected storage backend.

    Args:
        storage_mode (str, optional): The target storage system, either "DATABASE" or "GDRIVE".
            Defaults to "GDRIVE".
        db_uri_key (str, optional): The environment variable key for the database URI.
            Defaults to "DB_URI".
        gcp_credentials_key (str, optional): The environment variable key for GCP credentials.
            Defaults to "GCP_CREDENTIALS".
        drive_folder_id_key (str, optional): The environment variable key for the Drive folder ID.
            Defaults to "DRIVE_FOLDER_ID".

    Returns:
        Any: The SQLAlchemy engine instance if "DATABASE", or a tuple containing the
        Google Drive service instance and folder ID if "GDRIVE". Returns None on failure.
    """
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
    """Retrieve the list of previously processed video IDs from the designated storage.

    Args:
        storage_mode (str, optional): The target storage system, either "DATABASE" or "GDRIVE".
            Defaults to "GDRIVE".
        db_engine (Any, optional): The active SQLAlchemy database engine. Required if
            storage_mode is "DATABASE". Defaults to None.
        drive_service (Any, optional): The authenticated Google Drive service instance.
            Required if storage_mode is "GDRIVE". Defaults to None.
        folder_id (str | None, optional): The Google Drive folder ID. Required if
            storage_mode is "GDRIVE". Defaults to None.

    Returns:
        list[str]: A list of video IDs that have already been processed.

    Raises:
        ValueError: If the required connection objects for the selected storage mode are not provided.
    """
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
) -> None:
    """Route and store the extracted raw metadata batches to the appropriate storage backend.

    Args:
        fetched_snippets_and_stats (list[dict]): The batch of fetched video snippets and statistics.
        fetched_processed_vids (list[dict]): The batch of newly identified processed video records.
        fetched_top_comments (list[dict]): The batch of fetched top comments.
        storage_mode (str): The target storage system, either "DATABASE" or "GDRIVE".
        db_engine (Any, optional): The active SQLAlchemy database engine. Required if
            storage_mode is "DATABASE". Defaults to None.
        drive_service (Any, optional): The authenticated Google Drive service instance.
            Required if storage_mode is "GDRIVE". Defaults to None.
        folder_id (str | None, optional): The Google Drive folder ID. Required if
            storage_mode is "GDRIVE". Defaults to None.

    Raises:
        ValueError: If an unsupported storage mode is provided, or if the necessary
            connection objects for the chosen mode are missing.
    """
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
            max_days=None,
        )
        update_to_drive_jsonl(
            drive_service,
            folder_id,
            new_data=fetched_snippets_and_stats,
            filename="snippets_and_stats.jsonl",
            max_days=3,
        )
        update_to_drive_jsonl(
            drive_service,
            folder_id,
            new_data=fetched_top_comments,
            filename="top_comments.jsonl",
            max_days=3,
        )

    else:
        raise ValueError(f"Unsupported storage_mode: {storage_mode}")
