"""Extract and load YouTube video transcripts into a structured database schema."""

import os
import random
import time

from loguru import logger
from sqlalchemy import create_engine, text
import typer
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi

from src.load.db.connection import is_connected_to_db
from src.load.db.storage import append_to_db

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
        time.sleep(random.uniform(57.0, 67.3))
        ytt_api = YouTubeTranscriptApi()
        transcript = ytt_api.list(video_id).find_transcript(["en", "ko"])
        full_transcript = " ".join([seg.text for seg in transcript.fetch()]).replace("\n", " ")
        logger.success(f"Transcript fetched successfully for {video_id}")
        return full_transcript
    except NoTranscriptFound:
        logger.debug(f"No EN/KO transcript found for {video_id}.")
        return None
    except TranscriptsDisabled:
        logger.debug(f"Transcript completely disabled for {video_id}.")
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
) -> None:
    """Execute the extraction pipeline for video transcripts directly to the target database.

    Args:
        limit (int, optional): Restrict the number of transcripts processed per run. Defaults to 40.
        uri_key (str, optional): The environment variable key mapped to the database connection URI.
    """
    if limit > 40:
        logger.debug("Cannot fetch more than 40 transcripts.")
        limit = 40
        logger.info("Proceeding with 40 transcript limit...")

    # Check database connection
    db_uri_connection = is_connected_to_db(uri_key)
    if not db_uri_connection:
        return

    # Retrieve the database URL
    db_uri = os.environ.get(uri_key, "")

    logger.info("Starting to fetch video transcripts...")

    engine = create_engine(db_uri)

    try:
        with engine.connect() as conn:
            query = text("""
                SELECT s.video_id
                FROM skz_snippets s
                LEFT JOIN skz_transcripts t ON s.video_id = t.video_id
                WHERE s.video_format IN ('Long-form', 'Live/VOD')
                  AND t.video_id IS NULL
                LIMIT :limit
            """)

            # Execute and extract the list of strings
            result = conn.execute(query, {"limit": limit})
            ids_to_fetch = result.scalars().all()

    except Exception as e:
        logger.error(f"DB Read Error during transcript init: {e}")
        return

    if not ids_to_fetch:
        logger.info("No new long-form/live transcripts to fetch. Exiting.")
        return

    logger.info(f"Found missing long-form/live transcripts. Fetching up to {len(ids_to_fetch)}...")

    transcript_data = []
    unavailable_transcript_count = 0

    total_vids = len(ids_to_fetch)
    for idx, vid in enumerate(ids_to_fetch, start=1):
        logger.info(f"Fetching Transcript {idx}/{total_vids}... [Video ID: {vid}]")

        transcript_text = fetch_video_transcript(vid)
        if transcript_text != "FAILED_FETCH":
            transcript_data.append({"video_id": vid, "transcript": transcript_text})

        if transcript_text is None:
            unavailable_transcript_count += 1

    append_to_db(transcript_data, "skz_transcripts", db_uri)

    if unavailable_transcript_count > 0:
        logger.debug(
            f"{unavailable_transcript_count} unavailable transcript(s) found and appended as NULL(s)."
        )

    logger.success(f"Successfully fetched and appended {len(transcript_data)} transcripts.")


if __name__ == "__main__":
    app()
