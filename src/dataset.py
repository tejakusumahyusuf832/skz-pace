"""Extract and process YouTube video metadata and comments.

Utilize the YouTube Data API v3 to fetch video statistics, metadata,
top comments, and video transcripts for a specified YouTube channel.
Ensure state persistence to manage API quotas and network interruptions.
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import time
from typing import List

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger
import pandas as pd
from tqdm import tqdm
import typer
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi

from src.config import RAW_DATA_DIR

CHECKPOINT_FILE = RAW_DATA_DIR / "extraction_checkpoint.json"

app = typer.Typer()


def get_youtube_client():
    """Initialize and return the YouTube Data API client.

    Require the 'YOUTUBE_API_KEY' environment variable to be set prior
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
        List[str]: A list of video IDs, or an empty list if the checkpoint
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
    metadata_output: Path = RAW_DATA_DIR / "skz_metadata.parquet",
    stats_output: Path = RAW_DATA_DIR / "skz_stats.parquet",
    comments_output: Path = RAW_DATA_DIR / "skz_comments.parquet",
    transcripts_output: Path = RAW_DATA_DIR / "skz_transcripts.parquet",
    channel_id: str = "UC9rMiEjNaCSsebs31MRDCRA",
    update_static: bool = typer.Option(
        False,
        "--update-static",
        help="Force update of static metadata and transcripts for all videos.",
    ),
):
    """Execute the primary data extraction pipeline.

    Args:
        metadata_output (Path, optional): Filepath for static video metadata.
        stats_output (Path, optional): Filepath for dynamic video statistics.
        comments_output (Path, optional): Filepath for top-level comments.
        transcripts_output (Path, optional): Filepath for video transcripts.
        channel_id (str, optional): The target YouTube channel ID.
        update_static (bool, optional): Force update of static data for all videos.
    """

    try:
        youtube = get_youtube_client()
    except ValueError:
        return

    base_id = channel_id.replace("UC", "")
    playlists_to_process = {
        "Long-form": f"UULF{base_id}",
        "Short": f"UUSH{base_id}",
        "Live/VOD": f"UULV{base_id}",
    }

    processed_ids = load_checkpoint()
    all_current_videos = []

    for video_format, playlist_id in playlists_to_process.items():
        logger.info(f"Fetching IDs for {video_format} from {playlist_id}...")
        try:
            video_ids = get_all_video_ids(youtube, playlist_id)
            for vid in video_ids:
                all_current_videos.append({"id": vid, "format": video_format})
        except HttpError as e:
            logger.warning(f"Could not fetch playlist {playlist_id}. Error: {e}")

    logger.info(f"Total videos on channel: {len(all_current_videos)}")

    metadata_records = []
    stats_records = []
    comment_data = []
    transcript_data = []

    scraped_at = datetime.now(timezone.utc).isoformat()

    try:
        chunk_size = 50
        for i in tqdm(
            range(0, len(all_current_videos), chunk_size), desc="Processing Videos (Batched)"
        ):
            batch = all_current_videos[i : i + chunk_size]
            batch_ids = [v["id"] for v in batch]
            batch_formats = {v["id"]: v["format"] for v in batch}

            # Group video IDs to minimize API calls and stay within quota constraints
            vid_response = (
                youtube.videos().list(part="snippet,statistics", id=",".join(batch_ids)).execute()
            )

            for vid_info in vid_response.get("items", []):
                video_id = vid_info["id"]
                snippet = vid_info["snippet"]
                stats = vid_info.get("statistics", {})
                is_new_video = video_id not in processed_ids

                # Always append dynamic statistics to build a time-series history
                stats_records.append(
                    {
                        "video_id": video_id,
                        "scraped_at": scraped_at,
                        "view_count": int(stats.get("viewCount", 0)),
                        "like_count": int(stats.get("likeCount", 0)),
                        "comment_count": int(stats.get("commentCount", 0)),
                    }
                )

                # Fetch static data only for new videos to conserve API quota, unless forced
                if is_new_video or update_static:
                    metadata_records.append(
                        {
                            "video_id": video_id,
                            "published_at": snippet["publishedAt"],
                            "video_format": batch_formats[video_id],
                            "title": snippet["title"],
                            "description": snippet["description"],
                            "category_id": snippet["categoryId"],
                            "tags": ",".join(snippet.get("tags", [])),
                        }
                    )

                    # Transcripts are computationally heavy; restrict to formats likely to contain them
                    if batch_formats[video_id] in ["Long-form", "Live/VOD"]:
                        try:
                            time.sleep(random.uniform(1.2, 3.0))

                            ytt_api = YouTubeTranscriptApi()
                            transcript = ytt_api.list(video_id).find_transcript(["en", "ko"])
                            full_transcript = " ".join(
                                [seg.text for seg in transcript.fetch()]
                            ).replace("\n", " ")
                        except NoTranscriptFound:
                            logger.debug(f"No EN/KO transcript found for {video_id}.")
                            full_transcript = "na"
                        except TranscriptsDisabled:
                            logger.debug(f"Transcripts completely disabled for {video_id}.")
                            full_transcript = "na"
                        except Exception as e:
                            logger.warning(
                                f"Unexpected transcript error for {video_id}: {type(e).__name__} - {e}"
                            )
                            full_transcript = "na"

                        transcript_data.append(
                            {"video_id": video_id, "transcript": full_transcript}
                        )

                # Always append comments to capture shifts in top relevance rankings over time
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
                                "scraped_at": scraped_at,  # Track timestamp to record when this specific ranking was observed
                            }
                        )
                except HttpError as e:
                    if e.resp.status == 403 and "quotaExceeded" in str(e):
                        logger.warning("Quota Exceeded on comments. Saving state and halting.")
                        raise e
                    elif e.resp.status == 403:
                        logger.debug(f"Comments disabled for video {video_id}.")
                    else:
                        logger.error(f"Error fetching comments for {video_id}: {e}")

                if is_new_video:
                    processed_ids.append(video_id)

            save_checkpoint(processed_ids)

        logger.success("Extraction completed successfully.")

    except Exception as e:
        logger.error(f"Extraction halted: {e}")

    finally:
        save_checkpoint(processed_ids)

        def append_to_parquet(data_list: List[dict], filepath: Path):
            """Append a list of dictionaries to a Parquet file safely.

            Create a new Parquet file if one does not exist, otherwise concatenate
            the new data with the existing dataset.

            Args:
                data_list (List[dict]): The dataset to append.
                filepath (Path): The destination Parquet file path.
            """
            if data_list:
                df_new = pd.DataFrame(data_list)
                if filepath.exists():
                    df_existing = pd.read_parquet(filepath)
                    df_new = pd.concat([df_existing, df_new], ignore_index=True)
                df_new.to_parquet(filepath, index=False)
                logger.info(f"Appended {len(data_list)} records to {filepath}")

        append_to_parquet(stats_records, stats_output)
        append_to_parquet(metadata_records, metadata_output)
        append_to_parquet(comment_data, comments_output)
        append_to_parquet(transcript_data, transcripts_output)


if __name__ == "__main__":
    app()
