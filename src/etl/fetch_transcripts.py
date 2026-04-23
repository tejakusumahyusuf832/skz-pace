import os
import random
import time

from loguru import logger
import polars as pl
from tqdm import tqdm
import typer
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi

from src.db.connection import is_connected_to_db
from src.db.storage import append_to_db

app = typer.Typer()


def fetch_video_transcript(video_id: str) -> str | None:
    """Fetch the English or Korean transcript for a specific video.

    Args:
        video_id (str): The specific video ID to query.

    Returns:
        str | None: The concatenated full transcript text. Returns "FAILED_FETCH" if
            an unexpected error occurs indicating the append should be skipped, or
            None if the transcript is definitively unavailable or disabled.
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
        return None
    except TranscriptsDisabled:
        logger.debug(f"Transcripts completely disabled for {video_id}.")
        return None
    except Exception as e:
        logger.warning(f"Unexpected transcript error for {video_id}: {type(e).__name__} - {e}")
        return "FAILED_FETCH"


@app.command()
def main(
    limit: int = typer.Option(
        40, help="Maximum number of transcripts to fetch. Cannot exceed 40."
    ),
    uri_key: str = typer.Option("URI_KEY", help="The .env key containing the DB URI."),
):
    # Check max number of transcripts to fetch
    # Can only fetch up to 40 transcripts
    if limit > 40:
        logger.debug("Cannot fetch more than 40 transcripts.")
        limit = 40
        logger.info("Proceeding with 40 transcript limit...")

    # Check database connection
    db_uri_connection = is_connected_to_db(uri_key)
    if not db_uri_connection:
        return

    # Retrieve the database URI
    db_uri = os.environ.get(uri_key)
    if not db_uri:
        db_uri = ""

    try:
        df_proc = pl.read_database_uri(
            "SELECT video_id FROM skz_snippets", uri=db_uri, engine="connectorx"
        )
        processed_ids = df_proc["video_id"].to_list() if not df_proc.is_empty() else []
    except Exception as e:
        logger.warning(f"Could not read from DB. Proceeding with empty processed_ids. Error: {e}")
        processed_ids = []

    logger.info("Starting to fetch video transcripts...")
    already_fetched = set()
    valid_ids = set(processed_ids)

    try:
        # Query previously fetched transcripts to avoid duplication in database mode.
        df_fetched = pl.read_database_uri(
            "SELECT video_id FROM skz_transcripts", uri=db_uri, engine="connectorx"
        )
        already_fetched = (
            set(df_fetched["video_id"].to_list()) if not df_fetched.is_empty() else set()
        )

        df_valid = pl.read_database_uri(
            "SELECT video_id FROM skz_snippets WHERE video_format IN ('Long-form', 'Live/VOD')",
            uri=db_uri,
            engine="connectorx",
        )
        valid_ids = set(df_valid["video_id"].to_list()) if not df_valid.is_empty() else set()
    except Exception as e:
        logger.error(f"DB Read Error during transcript init: {e}")
        return

    missing_transcripts = [
        vid for vid in processed_ids if vid not in already_fetched and vid in valid_ids
    ]
    ids_to_fetch = missing_transcripts[:limit]

    if not ids_to_fetch:
        logger.info("No new long-form/live transcripts to fetch. Exiting.")
        return

    logger.info(
        f"Found {len(missing_transcripts)} missing long-form/live transcripts. Fetching up to {limit}..."
    )

    transcript_data = []
    unavailable_transcript_count = 0

    for vid in tqdm(ids_to_fetch, desc="Fetching Transcripts"):
        text = fetch_video_transcript(vid)
        if text != "FAILED_FETCH":
            transcript_data.append({"video_id": vid, "transcript": text})

        if text is None:
            unavailable_transcript_count += 1

    append_to_db(transcript_data, "skz_transcripts", db_uri)

    if unavailable_transcript_count > 0:
        logger.debug(
            f"{unavailable_transcript_count} unavailable transcript(s) found and appended as NULL(s)."
        )

    logger.success(f"Successfully fetched and appended {len(transcript_data)} transcripts.")


if __name__ == "__main__":
    app()
