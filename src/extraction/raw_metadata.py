"""Extract raw YouTube API metadata and load it into a cloud database.

Handles batched API requests with exponential backoff for rate limits,
fetching snippets, statistics, and top comments for specified channel playlists.
"""

from datetime import datetime, timezone
from enum import Enum
import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
import typer

from src.extraction.synchronization import (
    get_old_processed_ids,
    prepare_authentication,
    store_raw_metadata,
)

app = typer.Typer()


class StorageOptions(str, Enum):
    DATABASE = "DATABASE"
    GDRIVE = "GDRIVE"


def get_youtube_client(api_key: str) -> object:
    """Initialize and return the Google API client for YouTube v3.

    Args:
        api_key (str, optional): The environment variable key containing the
            Google Cloud API Key. Defaults to "YOUTUBE_API_KEY".

    Returns:
        object: An initialized googleapiclient.discovery.Resource instance.

    Raises:
        ValueError: If the specified environment variable is not found.
    """
    API_KEY = os.environ.get(api_key)
    if not API_KEY:
        logger.error(f"{api_key} environment variable not set.")
        raise ValueError("Missing API Key")
    return build("youtube", "v3", developerKey=API_KEY)


def get_all_video_ids(youtube, playlist_id: str) -> list[str]:
    """Fetch all video IDs contained within a specified YouTube playlist.

    Args:
        youtube (googleapiclient.discovery.Resource): The initialized YouTube API client.
        playlist_id (str): The ID of the playlist to query.

    Returns:
        List[str]: A list of video IDs extracted from the playlist.
    """
    video_ids = []
    next_page_token = None

    logger.info("Fetching all video IDs from the channel...")
    while True:
        response = (
            youtube.playlistItems()
            .list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            .execute()
        )

        video_ids.extend([item["contentDetails"]["videoId"] for item in response["items"]])
        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    logger.info(f"Successfully fetched {len(video_ids)} video IDs.")
    return video_ids


def is_server_error(exception) -> bool:
    """Evaluate if an exception is a server-side 5xx HTTP error.

    Args:
        exception (Exception): The exception raised during an API call.

    Returns:
        bool: True if the exception is an HttpError with a status code >= 500.
    """
    if isinstance(exception, HttpError):
        return exception.resp.status >= 500
    return False


@retry(
    retry=retry_if_exception(is_server_error),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    stop=stop_after_attempt(4),
    reraise=True,
)
def get_snippets_and_stats(youtube, batch_ids: list[str]) -> dict:
    """Fetch snippet and statistics data for a batch of YouTube videos.

    Args:
        youtube (object): The initialized YouTube API client.
        batch_ids (List[str]): A list of video IDs to query (max 50).

    Returns:
        dict: The raw JSON response payload from the YouTube API.
    """
    return youtube.videos().list(part="snippet,statistics", id=",".join(batch_ids)).execute()


@retry(
    retry=retry_if_exception(is_server_error),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    stop=stop_after_attempt(4),
    reraise=True,
)
def get_top_comments(youtube, video_id: str) -> dict:
    """Fetch the top relevant comments for a single YouTube video.

    Args:
        youtube (object): The initialized YouTube API client.
        video_id (str): The target video ID.

    Returns:
        dict: The raw JSON response payload containing comment threads.
    """
    return (
        youtube.commentThreads()
        .list(
            part="snippet",
            videoId=video_id,
            maxResults=50,
            order="relevance",
            textFormat="plainText",
        )
        .execute()
    )


def get_new_processed_vids(
    to_processed_vids: dict, old_processed_ids: list[str], scraped_at: str
) -> list[dict]:
    """Filter newly discovered videos against previously processed database IDs.

    Args:
        to_processed_vids (dict): Dictionary mapping video IDs to their formats.
        old_processed_ids (List[str]): List of video IDs already existing in the database.
        scraped_at (str): ISO formatted timestamp of the current scraping run.

    Returns:
        List[dict]: Formatted records of new videos ready for database insertion.
    """
    new_processed_vids = []

    for video_id, video_format in to_processed_vids.items():
        if video_id not in old_processed_ids:
            new_processed_vids.append(
                {
                    "video_id": video_id,
                    "video_format": video_format,
                    "first_scraped_at": scraped_at,
                }
            )
    return new_processed_vids


@app.command()
def main(
    storage_mode: str = typer.Option(
        StorageOptions.GDRIVE, help="Storage for the raw metadata. Either 'DATABASE' or 'GDRIVE'"
    ),
    db_uri_key: str = typer.Option("DB_URI", help="The .env key containing the database URI"),
    gcp_credentials_key: str = typer.Option(
        "GCP_CREDENTIALS", help="The .env key containing the GCP credentials"
    ),
    folder_id_key: str = typer.Option(
        "DRIVE_FOLDER_ID", help="The .env key containing the Drive folder ID"
    ),
    api_key: str = typer.Option("YOUTUBE_API_KEY", help="The .env key containing the API key"),
    channel_id: str = typer.Option(
        "UC9rMiEjNaCSsebs31MRDCRA", help="The channel ID of the specified YouTube channel"
    ),
) -> None:
    """Execute the main ETL extraction pipeline for YouTube metadata.

    Args:
        uri_key (str, optional): The database connection URI key.
        api_key (str, optional): The YouTube API developer key.
        channel_id (str, optional): The target YouTube channel ID.
    """
    # Prepare the synchronization process
    if storage_mode == "DATABASE":
        db_engine = prepare_authentication(storage_mode, db_uri_key=db_uri_key)
        if not db_engine:
            return
        old_processed_ids = get_old_processed_ids(storage_mode, db_engine=db_engine)

    else:
        drive_service, drive_folder_id = prepare_authentication(
            gcp_credentials_key=gcp_credentials_key,
            drive_folder_id_key=folder_id_key,
        )
        if not drive_service:
            return
        old_processed_ids = get_old_processed_ids(
            drive_service=drive_service, folder_id=drive_folder_id
        )

    # Get all playlist IDs
    base_id = channel_id.replace("UC", "")
    playlists_to_process = {
        "Long-form": f"UULF{base_id}",
        "Short": f"UUSH{base_id}",
        "Live/VOD": f"UULV{base_id}",
    }

    # Initialize the Google API client
    try:
        youtube = get_youtube_client(api_key)
    except ValueError:
        return

    # Get all video IDs based on the playlist IDs
    all_current_videos = []
    for video_format, playlist_id in playlists_to_process.items():
        logger.info(f"Fetching IDs for {video_format} from {playlist_id}...")
        try:
            video_ids = get_all_video_ids(youtube, playlist_id)
            for vid in video_ids:
                all_current_videos.append({"video_id": vid, "format": video_format})
        except HttpError as e:
            logger.warning(f"Could not fetch playlist {playlist_id}. Error: {e}")

    logger.info(f"Total videos on channel: {len(all_current_videos)}")

    # Fetch the raw metadata
    fetched_snippets_and_stats = []
    fetched_top_comments = []
    fetched_processed_vids = []

    try:
        chunk_size = 50
        total_videos = len(all_current_videos)
        total_batches = (total_videos + chunk_size - 1) // chunk_size

        for batch_num, i in enumerate(range(0, total_videos, chunk_size), start=1):
            logger.info(f"Processing Video Batch {batch_num}/{total_batches}...")

            batch = all_current_videos[i : i + chunk_size]
            batch_ids = [v["video_id"] for v in batch]
            batch_processed_vids = {v["video_id"]: v["format"] for v in batch}

            scraped_at = datetime.now(timezone.utc).isoformat()

            # Fetch video snippets and stats
            try:
                video_response = get_snippets_and_stats(youtube, batch_ids)
            except HttpError as e:
                logger.error(f"Error fetching video metadata: {e}")
                return

            fetched_snippets_and_stats.append(
                {"scraped_at": scraped_at, "video_response": video_response}
            )

            # Fetch top 50 comments for each video
            for vid in batch_ids:
                try:
                    comment_response = get_top_comments(youtube, vid)
                    fetched_top_comments.append(
                        {
                            "video_id": vid,
                            "comment_response": comment_response,
                            "scraped_at": scraped_at,
                        }
                    )
                except HttpError as e:
                    if e.resp.status == 403 and "quotaExceeded" in str(e):
                        logger.warning("Quota Exceeded on comments. Saving state and halting.")
                        raise e
                    elif e.resp.status == 403:
                        logger.debug(f"Comments disabled for video {vid}.")
                    elif e.resp.status == 400 and "processingFailure" in str(e):
                        logger.warning(
                            f"YouTube API glitch (400 processingFailure) for video {vid}. Skipping."
                        )
                    else:
                        logger.error(f"Error fetching comments for {vid}: {e}")

            # Fetch new processed videos
            new_processed_vids = get_new_processed_vids(
                batch_processed_vids, old_processed_ids, scraped_at
            )
            fetched_processed_vids.extend(new_processed_vids)

            logger.success(f"Video Batch {batch_num}/{total_batches} completed.")

    except Exception as e:
        logger.error(f"Extraction halted: {e}")
    finally:
        if storage_mode == "DATABASE":
            store_raw_metadata(
                fetched_snippets_and_stats,
                fetched_processed_vids,
                fetched_top_comments,
                "DATABASE",
                db_engine=db_engine,
            )
        else:
            store_raw_metadata(
                fetched_snippets_and_stats,
                fetched_processed_vids,
                fetched_top_comments,
                "GDRIVE",
                drive_service=drive_service,
                folder_id=drive_folder_id,
            )


if __name__ == "__main__":
    app()
