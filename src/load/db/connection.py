"""Manage database connection validation and engine creation."""

import os
from typing import Any

from loguru import logger
from sqlalchemy import create_engine
import typer

app = typer.Typer()


def is_connected_to_db(uri_key: str, return_bool: bool = True) -> Any:
    """Verify the database connection using a URI stored in environment variables.

    Args:
        uri_key (str): The environment variable key mapped to the target database URI.
        return_bool (bool, optional): Determine whether to return the engine object
            alongside the boolean status. Defaults to True.

    Returns:
        Any: A tuple containing a boolean representing connection success, and the
        SQLAlchemy engine instance if successful and requested, otherwise None.
    """
    db_uri = os.environ.get(uri_key, "")

    if not db_uri:
        logger.error(
            f"Key '{uri_key}' not found in environment to connect to your database! Check your .env file."
        )
        return False, None if return_bool else None

    try:
        engine = create_engine(db_uri)
        with engine.connect():
            logger.success(f"Successfully connected to database via {uri_key}.")
            return True, engine if return_bool else None

    except Exception as e:
        logger.error(f"Connection to database failed: {e}")
        return False, None if return_bool else None


@app.command()
def main(
    uri_key: str = typer.Argument("DATABASE_URL", help="The .env key containing the DB URI"),
) -> None:
    """Execute a database connection check via the command line interface.

    Args:
        uri_key (str): The environment variable key mapped to the target database URI.
    """
    is_connected_to_db(uri_key, return_bool=False)


if __name__ == "__main__":
    app()
