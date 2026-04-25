"""Database connection and validation utilities.

Provides functions and CLI commands to test and validate database
connections using environment variable configuration.
"""

import os

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine
import typer

load_dotenv()

app = typer.Typer()


def is_connected_to_db(uri_key: str, return_bool: bool = True) -> bool | None:
    """Verify the database connection using a URI stored in environment variables.

    Args:
        uri_key (str): The environment variable key containing the database URI.
        return_bool (bool, optional): Return a boolean result instead of None on failure.
            Defaults to True.

    Returns:
        bool | None: True if the connection succeeds. False if return_bool is True and
            the connection fails. None if return_bool is False and the connection fails.
    """
    db_uri = os.environ.get(uri_key)

    if not db_uri:
        logger.error(
            f"Key '{uri_key}' not found in environment to connect to your database! Check your .env file."
        )
        return False if return_bool else None

    try:
        engine = create_engine(db_uri)
        with engine.connect():
            logger.success(f"Successfully connected to database via {uri_key}.")
            return True if return_bool else None

    except Exception as e:
        logger.error(f"Connection to database failed: {e}")
        return False if return_bool else None


@app.command()
def main(
    uri_key: str = typer.Argument("DATABASE_URL", help="The .env key containing the DB URI"),
) -> None:
    """Execute a database connection check via the CLI.

    Args:
        uri_key (str): The environment variable key mapped to the target database URI.
    """
    is_connected_to_db(uri_key, return_bool=False)


if __name__ == "__main__":
    app()
