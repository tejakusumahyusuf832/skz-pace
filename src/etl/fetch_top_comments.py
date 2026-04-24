import os
from typing import Any, Sequence

from loguru import logger
from sqlalchemy import create_engine, text
import typer

from src.db.connection import is_connected_to_db
from src.db.storage import append_to_db

app = typer.Typer()


def process_comments(raw_data: Sequence[Any]) -> list:
    comment_data = []

    for row in raw_data:
        comment_response = row["comment_response"]
        scraped_at = row["scraped_at"]
        video_id = row["video_id"]

        for item in comment_response.get("items", []):
            top_comment = item["snippet"]["topLevelComment"]["snippet"]
            comment_data.append(
                {
                    "video_id": video_id,
                    "comment_id": item["id"],
                    "author": top_comment.get("authorDisplayName", "Unknown"),
                    "text": top_comment.get("textDisplay", ""),
                    "like_count": int(top_comment.get("likeCount", 0)),
                    "published_at": top_comment.get("publishedAt"),
                    "scraped_at": scraped_at,
                }
            )

    return comment_data


@app.command()
def main(
    uri_key_start: str = typer.Option("URI_KEY_START", help="DB URI key containing the raw data"),
    uri_key_end: str = typer.Option(
        "URI_KEY_END", help="DB URI key containing the transformed data"
    ),
):
    if not (is_connected_to_db(uri_key_start) and is_connected_to_db(uri_key_end)):
        return

    db_uri_start = os.environ.get(uri_key_start, "")
    db_uri_end = os.environ.get(uri_key_end, "")
    engine_start = create_engine(db_uri_start)
    engine_end = create_engine(db_uri_end)

    try:
        with engine_end.connect() as conn:
            result = conn.execute(text("SELECT MAX(scraped_at) FROM skz_top_comments"))
            last_scraped_at = result.scalar()
    except Exception:
        last_scraped_at = None

    query = "SELECT video_id, scraped_at, comment_response FROM top_comments"
    if last_scraped_at:
        query += f" WHERE scraped_at > '{last_scraped_at}'"
    query += " ORDER BY scraped_at ASC"

    with engine_start.connect() as conn:
        logger.info("Fetching new comments data from raw database...")
        raw_results = conn.execute(text(query)).mappings().all()

    if not raw_results:
        logger.info("No new comments to process.")
        return

    transformed_data = process_comments(raw_results)
    append_to_db(transformed_data, "skz_top_comments", db_uri_end)


if __name__ == "__main__":
    app()
