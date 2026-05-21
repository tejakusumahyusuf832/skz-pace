"""Transform and load raw YouTube snippet payloads into structured databases.

Pulls unprocessed JSON blobs from the cloud data lake, flattens the metadata
into tabular records, and performs upserts into the transformed schema.
"""

import os
from typing import Any, Sequence

from loguru import logger
from sqlalchemy import create_engine, text
import typer

from src.load.db.connection import is_connected_to_db

app = typer.Typer()


def upsert_snippets(data_list: list, db_uri: str) -> None:
    """Insert new video snippets or update existing ones on primary key conflict.

    Args:
        data_list (list): A list of dictionaries containing flattened snippet data.
        db_uri (str): The target database connection string.
    """
    if not data_list:
        logger.info("No snippet records to upsert. Skipping DB load.")
        return

    engine = create_engine(db_uri)

    query = text("""
        INSERT INTO skz_snippets 
        (video_id, published_at, video_format, title, description, category_id, tags, video_link, scraped_at)
        VALUES 
        (:video_id, :published_at, :video_format, :title, :description, :category_id, :tags, :video_link, :scraped_at)
        ON CONFLICT (video_id) 
        DO UPDATE SET 
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            category_id = EXCLUDED.category_id,
            tags = EXCLUDED.tags,
            scraped_at = EXCLUDED.scraped_at;
    """)

    try:
        with engine.begin() as conn:
            conn.execute(query, data_list)
        logger.success(f"Successfully upserted {len(data_list)} records to 'skz_snippets'")
    except Exception as e:
        logger.error(f"Database upsert failed for skz_snippets: {e}")


def process_snippets(raw_data: Sequence[Any], formats_map: dict) -> list:
    """Flatten raw API JSON payloads into standardized dictionary records.

    Args:
        raw_data (Sequence[Any]): Database row proxy objects containing the raw JSON.
        formats_map (dict): A mapping of video IDs to their respective formats.

    Returns:
        list: A list of dictionaries matching the 'skz_snippets' table schema.
    """
    snippet_records = []

    for row in raw_data:
        vid_response = row["video_response"]
        scraped_at = row["scraped_at"]

        for vid_info in vid_response.get("items", []):
            video_id = vid_info["id"]
            snippet = vid_info["snippet"]

            snippet_records.append(
                {
                    "video_id": video_id,
                    "published_at": snippet.get("publishedAt"),
                    "video_format": formats_map.get(video_id, "Unknown"),
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "category_id": snippet.get("categoryId"),
                    "tags": ",".join(snippet.get("tags", [])),
                    "video_link": f"https://www.youtube.com/watch?v={video_id}",
                    "scraped_at": scraped_at,
                }
            )

    return snippet_records


@app.command()
def main(
    uri_key_start: str = typer.Option("URI_KEY_START", help="DB URI key containing the raw data"),
    uri_key_end: str = typer.Option(
        "URI_KEY_END", help="DB URI key containing the transformed data"
    ),
) -> None:
    """Execute the extraction, transformation, and load process for video snippets.

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

    # Get the High-Water Mark to avoid pulling the entire cloud database every time
    try:
        with engine_end.connect() as conn:
            result = conn.execute(text("SELECT MAX(scraped_at) FROM skz_snippets"))
            last_scraped_at = result.scalar()
    except Exception as e:
        logger.warning(f"Could not read from local DB (table might be empty): {e}")
        last_scraped_at = None

    # Fetch format mapping from Neon DB
    formats_map = {}
    with engine_start.connect() as conn:
        result = conn.execute(text("SELECT video_id, video_format FROM processed_vids"))
        for row in result:
            formats_map[row[0]] = row[1]

    # Fetch NEW data from Neon DB
    query = "SELECT scraped_at, video_response FROM snippets_and_stats"
    if last_scraped_at:
        query += f" WHERE scraped_at > '{last_scraped_at}'"
    query += " ORDER BY scraped_at ASC"

    with engine_start.connect() as conn:
        logger.info("Fetching new snippet data from raw database...")
        raw_results = conn.execute(text(query)).mappings().all()

    if not raw_results:
        logger.info("No new snippets to process.")
        return

    # Transform and Upsert
    logger.info(f"Transforming {len(raw_results)} new batches...")
    transformed_data = process_snippets(raw_results, formats_map)

    # Use our new upsert function instead of append_to_db!
    upsert_snippets(transformed_data, db_uri_end)


if __name__ == "__main__":
    app()
