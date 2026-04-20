from typing import List

from googleapiclient.errors import HttpError
from loguru import logger
from tqdm import tqdm

from src.etl.youtube_auth import get_youtube_client


def extract_top_comments(video_ids: List[str], scraped_at: str) -> List[dict]:
    """Fetch top-level comments for a list of specific videos.

    Args:
        video_ids (List[str]): List of video IDs to query.
        scraped_at (str): ISO 8601 timestamp for the extraction run.

    Returns:
        List[dict]: A list of dictionaries containing top comment records.
    """
    youtube = get_youtube_client()
    comment_data = []

    for video_id in tqdm(video_ids, desc="Fetching Top Comments"):
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
                logger.warning("Quota Exceeded on comments. Halting comment extraction.")
                break  # Break out of loop, return what we have so far
            elif e.resp.status == 403:
                logger.debug(f"Comments disabled for video {video_id}.")
            elif e.resp.status == 400 and "processingFailure" in str(e):
                logger.warning(
                    f"YouTube API glitch (400 processingFailure) for {video_id}. Skipping."
                )
            else:
                logger.error(f"Error fetching comments for {video_id}: {e}")

    return comment_data
