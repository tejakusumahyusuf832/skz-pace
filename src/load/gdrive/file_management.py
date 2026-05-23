import io
import json
import os
import tempfile
from typing import Any

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from loguru import logger


def get_file_id_by_name(service: Any, filename: str, folder_id: str) -> str | None:
    """Search for a file by name in a specific folder and return its ID."""
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    items = results.get("files", [])

    return items[0]["id"] if items else None


def _download_file(service, file_path, file_id):
    request = service.files().get_media(fileId=file_id)
    file_stream = io.FileIO(file_path, "wb")
    downloader = MediaIoBaseDownload(file_stream, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        logger.info(f"Download {int(status.progress() * 100)}%.")
    file_stream.close()


def load_jsonl_file(
    service,
    *,
    filename: str,
    folder_id: str,
    desired_keys: None | str | list = None,
) -> list:
    file_id = get_file_id_by_name(service, filename, folder_id)

    if not file_id:
        logger.info(f"No existing {filename} found. Returning an empty list...")
        return []

    jsonl_data = []
    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = os.path.join(temp_dir, filename)
        _download_file(service, local_path, file_id)

        with open(local_path, "r") as file:
            if isinstance(desired_keys, str):
                for line in file:
                    record = json.loads(line)
                    filtered_record = record.get(desired_keys)
                    jsonl_data.append(filtered_record)
            elif isinstance(desired_keys, list):
                for line in file:
                    record = json.loads(line)
                    filtered_record = {key: record.get(key) for key in desired_keys}
                    jsonl_data.append(filtered_record)
            elif not desired_keys:
                for line in file:
                    record = json.loads(line)
                    jsonl_data.append(record)

    return jsonl_data


def save_to_drive_jsonl(service: Any, folder_id: str, *, new_data: list, filename: str):
    file_id = get_file_id_by_name(service, filename, folder_id)
    temp_dir = tempfile.gettempdir()
    local_path = os.path.join(temp_dir, filename)

    # Stream the download DIRECTLY to disk (Zero RAM parsing)
    if file_id:
        logger.info(f"Downloading {filename} to disk for appending...")
        try:
            request = service.files().get_media(fileId=file_id)
            with open(local_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
        except Exception as e:
            logger.error(f"Failed to download {filename}. Starting fresh. Error: {e}")

    # Append the new data directly to the file on disk
    logger.info(f"Appending {len(new_data)} new records to {filename}...")
    # Open in 'a' (append) mode. If the file doesn't exist yet, 'a' creates it automatically!
    with open(local_path, "a", encoding="utf-8") as f:
        for item in new_data:
            # json.dumps converts the dictionary to a string, and we add a newline
            f.write(json.dumps(item) + "\n")

    # Upload the modified file back to Drive
    media = MediaFileUpload(local_path, mimetype="application/json")

    try:
        if file_id:
            logger.info(f"Overwriting {filename} on Drive...")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            logger.info(f"Creating new {filename} on Drive...")
            file_metadata = {"name": filename, "parents": [folder_id]}
            service.files().create(body=file_metadata, media_body=media).execute()
        logger.success(f"Successfully saved {filename}.")
    except Exception as e:
        logger.error(f"Failed to save {filename} to Drive: {e}")
    finally:
        # Clean up the local hard drive
        if os.path.exists(local_path):
            os.remove(local_path)
