"""Extract and process YouTube video snippets, stats, comments, & transcripts.

Utilize the YouTube Data API v3 to fetch video snippets, statistics, top
comments, and transcripts for a specified channel. Manage state persistence
to handle API quotas and network interruptions.
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import time
from typing import List, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger
import polars as pl
from tqdm import tqdm
import typer
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi

from src.config import RAW_DATA_DIR

CHECKPOINT_FILE = RAW_DATA_DIR / "extraction_checkpoint.json"

app = typer.Typer()


def get_youtube_client():
    """Initialize and return the YouTube Data API client.

    Returns:
        googleapiclient.discovery.Resource: The authenticated YouTube API client.

    Raises:
        ValueError: If the 'YOUTUBE_API_KEY' environment variable is not set.
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
        channel_id (str, optional): The unique identifier of the YouTube channel.
            Defaults to "UC9rMiEjNaCSsebs31MRDCRA".

    Returns:
        str: The playlist ID corresponding to the channel's uploads.
    """
    response = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_all_video_ids(youtube, playlist_id: str) -> List[str]:
    """Retrieve all video IDs associated with a specific playlist.

    Args:
        youtube (googleapiclient.discovery.Resource): The initialized YouTube API client.
        playlist_id (str): The unique identifier of the YouTube playlist.

    Returns:
        List[str]: A list of video IDs contained within the playlist.
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
        List[str]: A list of video IDs that have already been processed. Returns an
            empty list if the checkpoint file does not exist.
    """
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return []


def save_checkpoint(processed_ids: List[str]):
    """Save the current list of processed video IDs to disk to persist state.

    Args:
        processed_ids (List[str]): The list of video IDs to save.
    """
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(processed_ids, f)


def append_to_parquet(data_list: List[dict], filepath: Path):
    """Append a list of dictionaries to a Parquet file.

    Create a new Parquet file if one does not exist; otherwise, concatenate
    the new data with the existing dataset.

    Args:
        data_list (List[dict]): The data records to append.
        filepath (Path): The file path to the target Parquet file.
    """
    if data_list:
        df_new = pl.DataFrame(data_list)
        if filepath.exists():
            df_existing = pl.read_parquet(filepath)
            df_new = pl.concat([df_existing, df_new], how="vertical")
        df_new.write_parquet(filepath)
        logger.info(f"Appended {len(data_list)} records to {filepath}")


def fetch_video_metadata(
    youtube,
    batch_ids: List[str],
    batch_formats: dict,
    processed_ids: List[str],
    scraped_at: str,
    update_snippets: bool,
) -> Tuple[List[dict], List[dict], List[str]]:
    """Fetch snippet and statistics metadata for a batch of videos.

    Args:
        youtube (googleapiclient.discovery.Resource): The initialized YouTube API client.
        batch_ids (List[str]): A list of video IDs to query.
        batch_formats (dict): A mapping of video IDs to their respective formats.
        processed_ids (List[str]): A list of previously processed video IDs.
        scraped_at (str): The ISO 8601 timestamp representing the extraction time.
        update_snippets (bool): Flag indicating whether to force update snippets
            for existing videos.

    Returns:
        Tuple[List[dict], List[dict], List[str]]: A tuple containing the extracted statistics
            records, snippet records, and a list of newly encountered video IDs.
    """
    stats_records = []
    snippet_records = []
    new_ids = []

    try:
        vid_response = (
            youtube.videos().list(part="snippet,statistics", id=",".join(batch_ids)).execute()
        )
    except HttpError as e:
        logger.error(f"Error fetching video metadata: {e}")
        return [], [], []

    for vid_info in vid_response.get("items", []):
        video_id = vid_info["id"]
        snippet = vid_info["snippet"]
        stats = vid_info.get("statistics", {})
        is_new_video = video_id not in processed_ids

        stats_records.append(
            {
                "video_id": video_id,
                "scraped_at": scraped_at,
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
            }
        )

        if is_new_video or update_snippets:
            snippet_records.append(
                {
                    "video_id": video_id,
                    "published_at": snippet["publishedAt"],
                    "video_format": batch_formats[video_id],
                    "title": snippet["title"],
                    "description": snippet["description"],
                    "category_id": snippet["categoryId"],
                    "tags": ",".join(snippet.get("tags", [])),
                    "scraped_at": scraped_at,
                }
            )

        if is_new_video:
            new_ids.append(video_id)

    return stats_records, snippet_records, new_ids


def fetch_top_comments(youtube, video_id: str, scraped_at: str) -> List[dict]:
    """Fetch top-level comments for a specific video.

    Args:
        youtube (googleapiclient.discovery.Resource): The initialized YouTube API client.
        video_id (str): The specific video ID to query.
        scraped_at (str): The ISO 8601 timestamp representing the extraction time.

    Returns:
        List[dict]: A list of dictionaries containing comment data. Returns an
            empty list if comments are disabled or an error occurs.

    Raises:
        HttpError: If an API quota limitation is exceeded.
    """
    comment_data = []
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
                    "like_count": int(top_comment.get("likeCount", 0)),
                    "published_at": top_comment["publishedAt"],
                    "scraped_at": scraped_at,
                }
            )
    except HttpError as e:
        if e.resp.status == 403 and "quotaExceeded" in str(e):
            logger.warning("Quota Exceeded on comments. Saving state and halting.")
            raise e
        elif e.resp.status == 403:
            logger.debug(f"Comments disabled for video {video_id}.")
        elif e.resp.status == 400 and "processingFailure" in str(e):
            logger.warning(
                f"YouTube API glitch (400 processingFailure) for video {video_id}. Skipping."
            )
        else:
            logger.error(f"Error fetching comments for {video_id}: {e}")

    return comment_data


def fetch_video_transcript(video_id: str) -> str:
    """Fetch the English or Korean transcript for a specific video.

    Args:
        video_id (str): The specific video ID to query.

    Returns:
        str: The concatenated full transcript text, or a standardized error
            string if unavailable.
    """
    try:
        # Introduce jitter to mitigate temporary rate-limiting or IP blocking
        # from the undocumented transcript API.
        time.sleep(random.uniform(57.0, 67.3))
        ytt_api = YouTubeTranscriptApi()
        transcript = ytt_api.list(video_id).find_transcript(["en", "ko"])
        full_transcript = " ".join([seg.text for seg in transcript.fetch()]).replace("\n", " ")
        return full_transcript
    except NoTranscriptFound:
        logger.debug(f"No EN/KO transcript found for {video_id}.")
        return "na - No transcript found"
    except TranscriptsDisabled:
        logger.debug(f"Transcripts completely disabled for {video_id}.")
        return "na - Transcript disabled"
    except Exception as e:
        logger.warning(f"Unexpected transcript error for {video_id}: {type(e).__name__} - {e}")
        return "na - Failed to fetch transcript"


@app.command()
def main(
    snippet_output: Path = RAW_DATA_DIR / "skz_snippets.parquet",
    stats_output: Path = RAW_DATA_DIR / "skz_stats.parquet",
    comments_output: Path = RAW_DATA_DIR / "skz_comments.parquet",
    transcripts_output: Path = RAW_DATA_DIR / "skz_transcripts.parquet",
    channel_id: str = "UC9rMiEjNaCSsebs31MRDCRA",
    update_snippets: bool = typer.Option(
        False,
        "--update-snippets",
        help="Force update of snippets (title, tags, etc.) for all videos.",
    ),
    fetch_transcripts: bool = typer.Option(
        False,
        "--fetch-transcripts",
        help="Only fetch missing transcripts using already extracted video IDs.",
    ),
):
    """Execute the primary data extraction pipeline.

    Args:
        snippet_output (Path, optional): Filepath for video snippets Parquet output.
        stats_output (Path, optional): Filepath for video statistics Parquet output.
        comments_output (Path, optional): Filepath for video comments Parquet output.
        transcripts_output (Path, optional): Filepath for video transcripts Parquet output.
        channel_id (str, optional): The target YouTube channel ID.
        update_snippets (bool, optional): Flag to overwrite snippets of previously processed videos.
        fetch_transcripts (bool, optional): Flag to run the pipeline strictly for fetching missing transcripts.
    """
    processed_ids = load_checkpoint()
    scraped_at = datetime.now(timezone.utc).isoformat()

    if fetch_transcripts:
        logger.info("Running in transcript-only mode...")
        already_fetched = set()

        if transcripts_output.exists():
            df_existing = pl.read_parquet(transcripts_output)
            if "video_id" in df_existing.columns:
                already_fetched = set(df_existing["video_id"].unique().to_list())

        valid_ids = set(processed_ids)
        if snippet_output.exists():
            df_snippets = pl.read_parquet(snippet_output)
            if "video_id" in df_snippets.columns and "video_format" in df_snippets.columns:
                valid_df = df_snippets.filter(
                    pl.col("video_format").is_in(["Long-form", "Live/VOD"])
                )
                valid_ids = set(valid_df["video_id"].unique().to_list())

        missing_transcripts = [
            vid for vid in processed_ids if vid not in already_fetched and vid in valid_ids
        ]

        # Enforce max limit per run to prevent triggering anti-scraping blocks from the transcript module.
        ids_to_fetch = missing_transcripts[:40]

        if not ids_to_fetch:
            logger.info("No new long-form/live transcripts to fetch. Exiting.")
            return

        logger.info(
            f"Found {len(missing_transcripts)} missing long-form/live transcripts. Fetching up to 40..."
        )
        transcript_data = []

        for vid in tqdm(ids_to_fetch, desc="Fetching Transcripts"):
            text = fetch_video_transcript(vid)
            transcript_data.append({"video_id": vid, "transcript": text})

        append_to_parquet(transcript_data, transcripts_output)
        logger.success(f"Successfully fetched and appended {len(transcript_data)} transcripts.")
        return

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

    snippet_records = []
    stats_records = []
    comment_data = []

    try:
        chunk_size = 50
        for i in tqdm(
            range(0, len(all_current_videos), chunk_size), desc="Processing Videos (Batched)"
        ):
            batch = all_current_videos[i : i + chunk_size]
            batch_ids = [v["id"] for v in batch]
            batch_formats = {v["id"]: v["format"] for v in batch}

            stats_batch, snippet_batch, new_ids = fetch_video_metadata(
                youtube, batch_ids, batch_formats, processed_ids, scraped_at, update_snippets
            )

            stats_records.extend(stats_batch)
            snippet_records.extend(snippet_batch)

            # Comments are always fetched regardless of 'processed_ids' state to capture
            # engagement and rank shifts over time.
            for vid in batch_ids:
                comments_batch = fetch_top_comments(youtube, vid, scraped_at)
                comment_data.extend(comments_batch)

            for vid in new_ids:
                if vid not in processed_ids:
                    processed_ids.append(vid)

            save_checkpoint(processed_ids)

        logger.success("Extraction completed successfully.")

    except Exception as e:
        logger.error(f"Extraction halted: {e}")

    finally:
        save_checkpoint(processed_ids)
        append_to_parquet(stats_records, stats_output)
        append_to_parquet(snippet_records, snippet_output)
        append_to_parquet(comment_data, comments_output)


if __name__ == "__main__":
    app()
