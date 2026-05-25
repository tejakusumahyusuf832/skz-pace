from datetime import datetime, timedelta, timezone
import io
import json
import os
import tempfile
from typing import Any

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from loguru import logger


def get_file_id_by_name(service: Any, filename: str, folder_id: str) -> str | None:
    """Search for a file by name in a specific folder and return its ID."""
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    items = results.get("files", [])

    return items[0]["id"] if items else None


def should_keep_record(scraped_at_str, *, max_days=None, last_scraped_date=None):
    dt = datetime.fromisoformat(scraped_at_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    if last_scraped_date:
        return dt > last_scraped_date
    elif max_days:
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=max_days)
        return dt >= cutoff_time


def _download_file(service, file_path, file_id):
    request = service.files().get_media(fileId=file_id)
    with io.FileIO(file_path, "wb") as file_stream:
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()


def load_jsonl_file(
    service,
    *,
    filename: str,
    folder_id: str,
    desired_keys: None | str | list = None,
    filter_date_scraped=None,
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
                    if filter_date_scraped:
                        if should_keep_record(
                            record.get("scraped_at"), last_scraped_date=filter_date_scraped
                        ):
                            rec_dict = record.get(desired_keys)
                            jsonl_data.append(rec_dict)
                    else:
                        rec_dict = record.get(desired_keys)
                        jsonl_data.append(rec_dict)
            elif isinstance(desired_keys, list):
                for line in file:
                    record = json.loads(line)
                    if filter_date_scraped:
                        if should_keep_record(
                            record.get("scraped_at"), last_scraped_date=filter_date_scraped
                        ):
                            rec_dict = {key: record.get(key) for key in desired_keys}
                            jsonl_data.append(rec_dict)
                    else:
                        rec_dict = {key: record.get(key) for key in desired_keys}
                        jsonl_data.append(rec_dict)
            elif not desired_keys:
                for line in file:
                    record = json.loads(line)
                    if filter_date_scraped:
                        if should_keep_record(
                            record.get("scraped_at"), last_scraped_date=filter_date_scraped
                        ):
                            jsonl_data.append(record)
                    else:
                        jsonl_data.append(record)

    return jsonl_data


def filter_and_process_jsonl(old_local_path, new_local_path, new_data, should_filter: bool):
    with open(new_local_path, "w") as out_f:
        if os.path.exists(old_local_path):
            with open(old_local_path, "r") as in_f:
                for line in in_f:
                    if not should_filter:
                        out_f.write(line)
                        continue

                    try:
                        record = json.loads(line)
                        if should_keep_record(record.get("scraped_at")):
                            out_f.write(line)

                    except json.JSONDecodeError:
                        continue

        for item in new_data:
            out_f.write(json.dumps(item) + "\n")


def update_to_drive_jsonl(service: Any, folder_id: str, *, new_data: list, filename: str):
    file_id = get_file_id_by_name(service, filename, folder_id)
    should_filter = True if filename != "processed_vids.jsonl" else False

    with tempfile.TemporaryDirectory() as temp_dir:
        old_local_path = os.path.join(temp_dir, "old_" + filename)
        new_local_path = os.path.join(temp_dir, filename)

        _download_file(service, old_local_path, file_id)
        filter_and_process_jsonl(old_local_path, new_local_path, new_data, should_filter)

        with open(new_local_path, "rb") as f:
            media = MediaIoBaseUpload(f, mimetype="application/jsonlines", resumable=True)
            logger.info("Updating existing file on Google Drive...")
            service.files().update(fileId=file_id, media_body=media).execute()
