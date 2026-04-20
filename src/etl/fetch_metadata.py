from typing import Dict, List, Tuple

from googleapiclient.errors import HttpError
from loguru import logger
from tqdm import tqdm

from src.etl.youtube_auth import get_youtube_client


def get_all_video_ids(youtube, playlist_id: str) -> List[str]:
    """Fetch all video IDs contained within a specified YouTube playlist."""
    video_ids = []
    next_page_token = None

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

    return video_ids


def extract_metadata(
    channel_id: str, processed_ids: List[str], scraped_at: str, update_snippets: bool = False
) -> Tuple[List[dict], List[dict], List[str]]:
    """Fetch video snippets and statistics for the entire channel.

    Args:
        channel_id (str): The target YouTube channel ID.
        processed_ids (List[str]): List of video IDs previously loaded to DB.
        scraped_at (str): ISO 8601 timestamp for the extraction run.
        update_snippets (bool): Flag to overwrite snippets of existing videos.

    Returns:
        Tuple[List[dict], List[dict], List[str]]: A tuple of stats records,
            snippet records, and newly encountered video IDs.
    """
    youtube = get_youtube_client()
    base_id = channel_id.replace("UC", "")
    playlists_to_process = {
        "Long-form": f"UULF{base_id}",
        "Short": f"UUSH{base_id}",
        "Live/VOD": f"UULV{base_id}",
    }

    all_current_videos: List[Dict[str, str]] = []

    for video_format, playlist_id in playlists_to_process.items():
        logger.info(f"Fetching IDs for {video_format} from {playlist_id}...")
        try:
            video_ids = get_all_video_ids(youtube, playlist_id)
            for vid in video_ids:
                all_current_videos.append({"id": vid, "format": video_format})
        except HttpError as e:
            logger.warning(f"Could not fetch playlist {playlist_id}. Error: {e}")

    logger.info(f"Total videos identified on channel: {len(all_current_videos)}")

    stats_records = []
    snippet_records = []
    new_ids = []

    chunk_size = 50
    for i in tqdm(range(0, len(all_current_videos), chunk_size), desc="Extracting Metadata"):
        batch = all_current_videos[i : i + chunk_size]
        batch_ids = [v["id"] for v in batch]
        batch_formats = {v["id"]: v["format"] for v in batch}

        try:
            vid_response = (
                youtube.videos().list(part="snippet,statistics", id=",".join(batch_ids)).execute()
            )
        except HttpError as e:
            logger.error(f"Error fetching metadata batch: {e}")
            continue

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
