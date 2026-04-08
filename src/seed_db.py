"""Seed local PostgreSQL database with existing Parquet files."""

import os

from loguru import logger
import polars as pl

from src.config import RAW_DATA_DIR

DB_URI = os.environ.get("LOCAL_DB_URL")


def migrate_parquet_to_sql():
    if not DB_URI:
        logger.error("LOCAL_DB_URL is missing from .env file!")
        return

    # Map the target SQL table names to your local Parquet files
    files_to_migrate = {
        "skz_snippets": RAW_DATA_DIR / "skz_snippets.parquet",
        "skz_stats": RAW_DATA_DIR / "skz_stats.parquet",
        "skz_comments": RAW_DATA_DIR / "skz_comments.parquet",
        "skz_transcripts": RAW_DATA_DIR / "skz_transcripts.parquet",
    }

    for table_name, filepath in files_to_migrate.items():
        if filepath.exists():
            logger.info(f"Loading {filepath.name} into table '{table_name}'...")

            # Read the parquet file into memory
            df = pl.read_parquet(filepath)

            # Write directly to PostgreSQL.
            # 'if_table_exists="replace"' drops the table
            # if it's already there, so you can safely rerun this script if needed.
            df.write_database(
                table_name=table_name,
                connection=DB_URI,
                if_table_exists="replace",
                engine="sqlalchemy",
            )
            logger.success(f"Successfully migrated {len(df)} rows to '{table_name}'.")
        else:
            logger.warning(f"File not found: {filepath.name}. Skipping.")


if __name__ == "__main__":
    migrate_parquet_to_sql()
