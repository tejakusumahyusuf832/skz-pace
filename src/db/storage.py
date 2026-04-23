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
