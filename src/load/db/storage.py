"""Handle database insertion operations and routine raw data pruning."""

import json
from typing import Any, List

from loguru import logger
from sqlalchemy import text


def append_to_db(data_list: List[dict], table_name: str, engine: Any) -> None:
    """Append a collection of records to a designated database table.

    Args:
        data_list (List[dict]): A list of dictionaries representing the rows to insert.
        table_name (str): The name of the target database table.
        engine (Any): The SQLAlchemy engine instance connected to the target database.
    """
    if not data_list:
        logger.info(f"No records provided for {table_name}. Skipping DB load.")
        return

    # Serialize nested structures to strings for relational database compatibility
    for row in data_list:
        for key, value in row.items():
            if isinstance(value, dict) or isinstance(value, list):
                row[key] = json.dumps(value)

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


def prune_old_raw_data(engine: Any, days_old: int = 7) -> None:
    """Delete records older than a specified threshold from the raw data tables.

    Args:
        engine (Any): The SQLAlchemy engine instance connected to the raw database.
        days_old (int, optional): The age threshold in days for determining which
            records to delete. Defaults to 7.
    """
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
