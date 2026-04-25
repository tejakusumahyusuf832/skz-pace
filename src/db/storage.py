import json
from typing import List

from loguru import logger
from sqlalchemy import create_engine, text


def append_to_db(data_list: List[dict], table_name: str, db_uri: str) -> None:
    if not data_list:
        logger.info(f"No records provided for {table_name}. Skipping DB load.")
        return

    for row in data_list:
        for key, value in row.items():
            if isinstance(value, dict) or isinstance(value, list):
                row[key] = json.dumps(value)

    engine = create_engine(db_uri)

    columns = ", ".join(data_list[0].keys())
    binds = ", ".join([f":{k}" for k in data_list[0].keys()])
    query = text(f"INSERT INTO {table_name} ({columns}) VALUES ({binds})")

    try:
        with engine.begin() as conn:
            conn.execute(query, data_list)

        logger.success(
            f"Successfully appended {len(data_list)} records to DB table '{table_name}'"
        )
    except Exception as e:
        logger.error(f"Database append failed for {table_name}: {e}")


def prune_old_raw_data(db_uri: str, days_old: int = 7) -> None:
    """Deletes raw JSON data older than a specified number of days to save cloud storage."""
    engine = create_engine(db_uri)

    queries = {
        "snippets_and_stats": text(
            f"DELETE FROM snippets_and_stats WHERE scraped_at < NOW() - INTERVAL '{days_old} days'"
        ),
        "top_comments": text(
            f"DELETE FROM top_comments WHERE scraped_at < NOW() - INTERVAL '{days_old} days'"
        ),
    }

    try:
        with engine.begin() as conn:  # .begin() automatically handles the transaction commit
            for table_name, query in queries.items():
                result = conn.execute(query)
                logger.success(
                    f"Pruned {result.rowcount} records older than {days_old} days from '{table_name}'"
                )
    except Exception as e:
        logger.error(f"Failed to prune old data from cloud database: {e}")
