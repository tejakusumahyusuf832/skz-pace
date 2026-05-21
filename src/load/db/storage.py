"""Database storage operations for raw and processed analytics data.

Provides utilities for bulk data insertion, handling JSON serialization
for complex objects, and pruning old data to maintain storage efficiency.
"""

import json
from typing import List

from loguru import logger
from sqlalchemy import create_engine, text


def append_to_db(data_list: List[dict], table_name: str, db_uri: str) -> None:
    """Insert a list of dictionary records into a specified database table.

    Automatically serializes nested lists and dictionaries into JSON strings
    prior to insertion to align with PostgreSQL JSONB column requirements.

    Args:
        data_list (List[dict]): The records to insert, represented as dictionaries.
        table_name (str): The target database table name.
        db_uri (str): The connection string for the target database.
    """
    if not data_list:
        logger.info(f"No records provided for {table_name}. Skipping DB load.")
        return

    # Serialize nested structures for database compatibility
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
    """Delete raw JSON data older than a specified threshold to free up cloud storage.

    Args:
        db_uri (str): The connection string for the database to prune.
        days_old (int, optional): The age threshold in days for record deletion.
            Defaults to 7.
    """
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
