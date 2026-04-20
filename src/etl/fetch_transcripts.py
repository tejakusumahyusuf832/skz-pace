import random
import time
from typing import List

from loguru import logger
from tqdm import tqdm
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi


def extract_transcripts(video_ids: List[str]) -> List[dict]:
    """Fetch English or Korean transcripts for a list of videos.

    Args:
        video_ids (List[str]): The list of video IDs to fetch transcripts for.

    Returns:
        List[dict]: A list of dictionary records containing video_id and the transcript.
    """
    transcript_data = []
    ytt_api = YouTubeTranscriptApi()

    for video_id in tqdm(video_ids, desc="Fetching Transcripts"):
        try:
            # Jitter to mitigate temporary rate-limiting from the undocumented API
            time.sleep(random.uniform(57.0, 67.3))

            transcript = ytt_api.list(video_id).find_transcript(["en", "ko"])
            full_transcript = " ".join([seg.text for seg in transcript.fetch()]).replace("\n", " ")

            transcript_data.append({"video_id": video_id, "transcript": full_transcript})

        except NoTranscriptFound:
            logger.debug(f"No EN/KO transcript found for {video_id}.")
        except TranscriptsDisabled:
            logger.debug(f"Transcripts completely disabled for {video_id}.")
        except Exception as e:
            logger.warning(f"Unexpected transcript error for {video_id}: {type(e).__name__} - {e}")

    return transcript_data
