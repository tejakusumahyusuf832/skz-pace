import io
import json
import os
from typing import Any

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from loguru import logger

from src.gdrive.authentication import get_drive_service


def get_file_id_by_name(service: Any, filename: str, folder_id: str) -> str | None:
    """Search for a file by name in a specific folder and return its ID."""
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    items = results.get("files", [])

    return items[0]["id"] if items else None


def load_file(service: Any, filename: str, folder_id: str) -> list:
    """
    Load a file from Drive as a list of dictionaries, or
    return an empty list if it doesn't exist.
    """
    file_id = get_file_id_by_name(service, filename, folder_id)

    if not file_id:
        logger.info(f"No existing {filename} found. Starting fresh.")
        return []

    logger.info(f"Downloading existing state from {filename}...")
    request = service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    file_stream.seek(0)
    return json.loads(file_stream.read().decode("utf-8"))


def save_processed_state(service: Any, data: list, filename: str, folder_id: str):
    """Update the existing state file on Drive, or create it if it's missing."""
    local_path = f"/tmp/{filename}"

    # Save the updated list locally first
    with open(local_path, "w") as f:
        json.dump(data, f, indent=2)

    file_id = get_file_id_by_name(service, filename, folder_id)
    media = MediaFileUpload(local_path, mimetype="application/json")

    try:
        if file_id:
            logger.info(f"Overwriting existing {filename} on Drive...")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            logger.info(f"Creating new {filename} on Drive...")
            file_metadata = {"name": filename, "parents": [folder_id]}
            service.files().create(body=file_metadata, media_body=media).execute()
    except Exception as e:
        logger.error(f"Failed to save state to Drive: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


def upload_json_to_drive(data: list[dict], filename: str, folder_id: str):
    """Save data to a local JSON file and upload to Google Drive."""
    service = get_drive_service()

    # 1. Save locally first
    local_path = f"/tmp/{filename}"
    with open(local_path, "w") as f:
        json.dump(data, f, indent=2)

    # 2. Upload to Drive
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, mimetype="application/json")

    try:
        logger.info(f"Uploading {filename} to Google Drive...")
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        logger.success(f"Successfully uploaded {filename}. File ID: {file.get('id')}")
    except Exception as e:
        logger.error(f"Failed to upload to Drive: {e}")
    finally:
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)
