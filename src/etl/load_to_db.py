import os
from typing import List

from loguru import logger
import polars as pl


def append_to_neon_db(data_list: List[dict], table_name: str) -> None:
    """Convert records to Polars DataFrame and append to Neon PostgreSQL.

    Args:
        data_list (List[dict]): The data records to append.
        table_name (str): The name of the target database table.

    Raises:
        ValueError: If 'DATABASE_URL' is missing.
        RuntimeError: If the database write operation fails.
    """
    if not data_list:
        logger.info(f"No records provided for {table_name}. Skipping DB load.")
        return

    db_uri = os.environ.get("DATABASE_URL")
    if not db_uri:
        logger.critical("DATABASE_URL environment variable is missing!")
        raise ValueError("Cannot load to database: DATABASE_URL is unset.")

    df_new = pl.DataFrame(data_list)

    try:
        df_new.write_database(
            table_name=table_name,
            connection=db_uri,
            if_table_exists="append",
            engine="sqlalchemy",
        )
        logger.success(
            f"Successfully appended {len(data_list)} records to DB table '{table_name}'"
        )
    except Exception as e:
        logger.error(f"Database append failed for {table_name}: {e}")
        raise RuntimeError(f"Database insertion failed for {table_name}") from e
