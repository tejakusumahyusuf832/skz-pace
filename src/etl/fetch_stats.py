"""Transform and load raw YouTube performance statistics into structured databases.

Extracts batched JSON responses from the cloud raw data lake, isolates daily
time-series statistics (views, likes, comments), and writes them to the local schema.
"""

import os
from typing import Any, Sequence

from loguru import logger
from sqlalchemy import create_engine, text
import typer

from src.db.connection import is_connected_to_db
from src.db.storage import append_to_db

app = typer.Typer()


def process_stats(raw_data: Sequence[Any]) -> list:
    """Parse raw API statistics payloads into standardized flat dictionaries.

    Args:
        raw_data (Sequence[Any]): Database mapping proxy containing nested API JSON.

    Returns:
        list: A list of flattened statistic records ready for database insertion.
    """
    stats_records = []

    for row in raw_data:
        vid_response = row["video_response"]
        scraped_at = row["scraped_at"]

        for vid_info in vid_response.get("items", []):
            video_id = vid_info["id"]
            stats = vid_info.get("statistics", {})

            stats_records.append(
                {
                    "video_id": video_id,
                    "scraped_at": scraped_at,
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                }
            )

    return stats_records


@app.command()
def main(
    uri_key_start: str = typer.Option("URI_KEY_START", help="DB URI key containing the raw data"),
    uri_key_end: str = typer.Option(
        "URI_KEY_END", help="DB URI key containing the transformed data"
    ),
) -> None:
    """Execute the extraction, transformation, and load process for video statistics.

    Args:
        uri_key_start (str, optional): The origin database connection key.
        uri_key_end (str, optional): The destination database connection key.
    """
    if not (is_connected_to_db(uri_key_start) and is_connected_to_db(uri_key_end)):
        return

    db_uri_start = os.environ.get(uri_key_start, "")
    db_uri_end = os.environ.get(uri_key_end, "")
    engine_start = create_engine(db_uri_start)
    engine_end = create_engine(db_uri_end)

    try:
        with engine_end.connect() as conn:
            result = conn.execute(text("SELECT MAX(scraped_at) FROM skz_stats"))
            last_scraped_at = result.scalar()
    except Exception:
        last_scraped_at = None

    query = "SELECT scraped_at, video_response FROM snippets_and_stats"
    if last_scraped_at:
        query += f" WHERE scraped_at > '{last_scraped_at}'"
    query += " ORDER BY scraped_at ASC"

    with engine_start.connect() as conn:
        logger.info("Fetching new stats data from cloud...")
        raw_results = conn.execute(text(query)).mappings().all()

    if not raw_results:
        logger.info("No new stats to process.")
        return

    transformed_data = process_stats(raw_results)
    append_to_db(transformed_data, "skz_stats", db_uri_end)


if __name__ == "__main__":
    app()
