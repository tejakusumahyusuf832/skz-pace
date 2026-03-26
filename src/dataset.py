"""Extracts and processes YouTube video metadata and comments.

This script utilizes the YouTube Data API v3 to fetch video statistics,
metadata, and top comments for a specified channel. It implements hidden
playlist prefixes to automatically categorize videos by format and ensures
state persistence to manage API quotas and network interruptions.
"""

import json
import os
from pathlib import Path
from typing import List

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger
import pandas as pd
from tqdm import tqdm
import typer

from src.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

CHECKPOINT_FILE = RAW_DATA_DIR / "extraction_checkpoint.json"

app = typer.Typer()


def get_youtube_client():
    """Initialize and return the YouTube Data API client.

    Requires the 'YOUTUBE_API_KEY' environment variable to be set prior
    to execution.

    Returns:
        googleapiclient.discovery.Resource: The authenticated YouTube API client.

    Raises:
        ValueError: If the 'YOUTUBE_API_KEY' environment variable is missing.
    """
    API_KEY = os.environ.get("YOUTUBE_API_KEY")
    if not API_KEY:
        logger.error("YOUTUBE_API_KEY environment variable not set.")
        raise ValueError("Missing API Key")
    return build("youtube", "v3", developerKey=API_KEY)


def get_channel_uploads_playlist(youtube, channel_id: str = "UC9rMiEjNaCSsebs31MRDCRA") -> str:
    """Retrieve the primary 'Uploads' playlist ID for a given channel.

    Args:
        youtube (googleapiclient.discovery.Resource): The initialized YouTube API client.
        channel_id (str, optional): The target YouTube channel ID. Defaults to
            "UC9rMiEjNaCSsebs31MRDCRA".

    Returns:
        str: The playlist ID containing the channel's uploaded videos.
    """
    response = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_all_video_ids(youtube, playlist_id: str) -> List[str]:
    """Retrieve all video IDs associated with a specific playlist through pagination.

    Args:
        youtube (googleapiclient.discovery.Resource): The initialized YouTube API client.
        playlist_id (str): The target YouTube playlist ID.

    Returns:
        List[str]: A complete list of video IDs contained within the playlist.
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


def load_checkpoint() -> List[str]:
    """Load the list of previously processed video IDs from disk.

    Returns:
        List[str]: A list of video IDs. Returns an empty list if the checkpoint
            file does not exist.
    """
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return []


def save_checkpoint(processed_ids: List[str]):
    """Save the current list of processed video IDs to disk to persist state.

    Args:
        processed_ids (List[str]): The complete list of processed video IDs.
    """
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(processed_ids, f)


@app.command()
def main(
    videos_output: Path = RAW_DATA_DIR / "skz_videos.parquet",
    comments_output: Path = RAW_DATA_DIR / "skz_comments.parquet",
    channel_id: str = "UC9rMiEjNaCSsebs31MRDCRA",
):
    """Execute the primary data extraction pipeline.

    Orchestrates the fetching of video metadata, performance statistics, and
    top comments. Utilizes checkpointing to allow resumption of the extraction
    process across multiple runs. Processed data is appended to Parquet files.

    Args:
        videos_output (Path, optional): The destination file path for video metadata.
            Defaults to RAW_DATA_DIR / "skz_videos.parquet".
        comments_output (Path, optional): The destination file path for comments data.
            Defaults to RAW_DATA_DIR / "skz_comments.parquet".
        channel_id (str, optional): The target YouTube channel ID. Defaults to
            "UC9rMiEjNaCSsebs31MRDCRA".
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        youtube = get_youtube_client()
    except ValueError:
        return

    # Construct hidden playlist IDs by replacing the standard 'UC' prefix.
    # This exposes natively segregated formats (Long-form, Shorts, Live) without
    # requiring an extra API call per video to determine its format type.
    base_id = channel_id.replace("UC", "")
    playlists_to_process = {
        "Long-form": f"UULF{base_id}",
        "Short": f"UUSH{base_id}",
        "Live/VOD": f"UULV{base_id}",
    }

    processed_ids = load_checkpoint()
    videos_to_process = []

    for video_format, playlist_id in playlists_to_process.items():
        logger.info(f"Fetching IDs for {video_format} from {playlist_id}...")
        try:
            video_ids = get_all_video_ids(youtube, playlist_id)
            for vid in video_ids:
                if vid not in processed_ids:
                    videos_to_process.append((vid, video_format))
        except HttpError as e:
            logger.warning(f"Could not fetch playlist {playlist_id}. Error: {e}")

    logger.info(f"Starting extraction for {len(videos_to_process)} unprocessed videos...")

    video_data = []
    comment_data = []

    try:
        for video_id, video_format in tqdm(videos_to_process, desc="Processing Videos"):
            vid_response = youtube.videos().list(part="snippet,statistics", id=video_id).execute()

            if not vid_response["items"]:
                continue

            vid_info = vid_response["items"][0]
            snippet = vid_info["snippet"]
            stats = vid_info.get("statistics", {})

            video_data.append(
                {
                    "video_id": video_id,
                    "published_at": snippet["publishedAt"],
                    "video_format": video_format,
                    "title": snippet["title"],
                    "description": snippet["description"],
                    "category_id": snippet["categoryId"],
                    "tags": ",".join(snippet.get("tags", [])),
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                }
            )

            try:
                comment_response = (
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

                for item in comment_response.get("items", []):
                    top_comment = item["snippet"]["topLevelComment"]["snippet"]
                    comment_data.append(
                        {
                            "video_id": video_id,
                            "comment_id": item["id"],
                            "author": top_comment["authorDisplayName"],
                            "text": top_comment["textDisplay"],
                            "like_count": int(top_comment["likeCount"]),
                            "published_at": top_comment["publishedAt"],
                        }
                    )
            except HttpError as e:
                if e.resp.status == 403 and "quotaExceeded" in str(e):
                    logger.warning(
                        "YouTube API Quota Exceeded during comment extraction. Saving state and halting."
                    )
                    # Re-raise the exception strictly to jump to the finally block
                    # ensuring safe persistence of current in-memory datasets before process termination.
                    raise e
                elif e.resp.status == 403:
                    logger.debug(f"Comments disabled for video {video_id}.")
                else:
                    logger.error(f"Error fetching comments for {video_id}: {e}")

            processed_ids.append(video_id)

            if len(processed_ids) % 50 == 0:
                save_checkpoint(processed_ids)

        logger.success("Extraction completed successfully without hitting quota limits.")

    except HttpError as e:
        logger.warning(
            f"API Error encountered ({e.resp.status}). Halting extraction. Progress saved."
        )
        logger.debug(f"Detailed error reason: {e.reason}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    finally:
        save_checkpoint(processed_ids)

        if video_data:
            df_new_videos = pd.DataFrame(video_data)
            if videos_output.exists():
                df_existing = pd.read_parquet(videos_output)
                df_new_videos = pd.concat([df_existing, df_new_videos], ignore_index=True)
            df_new_videos.to_parquet(videos_output, index=False)
            logger.info(f"Saved {len(video_data)} new video records to {videos_output}")

        if comment_data:
            df_new_comments = pd.DataFrame(comment_data)
            if comments_output.exists():
                df_existing = pd.read_parquet(comments_output)
                df_new_comments = pd.concat([df_existing, df_new_comments], ignore_index=True)
            df_new_comments.to_parquet(comments_output, index=False)
            logger.info(f"Saved {len(comment_data)} new comment records to {comments_output}")


if __name__ == "__main__":
    app()
