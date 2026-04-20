import os

from googleapiclient.discovery import build
from loguru import logger


def get_youtube_client():
    """Initialize and return the YouTube Data API client.

    Returns:
        googleapiclient.discovery.Resource: The authenticated YouTube API service object.

    Raises:
        ValueError: If the 'YOUTUBE_API_KEY' environment variable is not set.
    """
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        logger.error("YOUTUBE_API_KEY environment variable not set.")
        raise ValueError("Missing API Key")
    return build("youtube", "v3", developerKey=api_key)
