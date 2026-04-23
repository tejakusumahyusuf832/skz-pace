import os

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine
import typer

load_dotenv()

app = typer.Typer()


def is_connected_to_db(uri_key: str, return_bool: bool = True):
    db_uri = os.environ.get(uri_key)

    if not db_uri:
        logger.error(
            f"Key '{uri_key}' not found in environment to connect to your database! Check your .env file."
        )
        return False if return_bool else None

    try:
        # Attempt to connect to the database
        engine = create_engine(db_uri)
        with engine.connect():
            logger.success(f"Successfully connected to database via {uri_key}.")
            return True if return_bool else None

    except Exception as e:
        logger.error(f"Connection to database failed: {e}")
        return False if return_bool else None


@app.command()
def main(uri_key: str = typer.Argument("DATABASE_URL", help="The .env key containing the DB URI")):
    is_connected_to_db(uri_key, return_bool=False)


if __name__ == "__main__":
    app()
