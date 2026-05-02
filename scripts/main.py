import os

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text
import typer

from src.db.connection import is_connected_to_db
from src.process import set_process

app = typer.Typer()

load_dotenv()


@app.command()
def main(
    db_uri_key: str = typer.Option("", help="Insert your database URI key from .env to process."),
    number: int = typer.Option(5, help="Insert any integer to process."),
):
    connected_to_db = is_connected_to_db(db_uri_key)
    if not connected_to_db:
        return

    db_uri = os.environ.get(db_uri_key, "")

    print("Hello, Yusuf Tejakusumah!")
    print("We will process the data in")
    set_process(end=number)

    engine = create_engine(db_uri)

    query = text("""
        SELECT 
            skz_snippets.title,
            ROUND(AVG(skz_stats.view_count)) AS avg_view_count,
            ROUND(AVG(skz_stats.like_count)) AS avg_like_count,
            skz_snippets.published_at
        FROM skz_stats
        LEFT JOIN skz_snippets
            ON skz_stats.video_id = skz_snippets.video_id
        WHERE
            (
                extract(
                    MONTH
                    FROM skz_snippets.published_at AT TIME ZONE 'UTC'
                ) = 3
            ) AND (
                extract(
                    DAY
                    FROM skz_snippets.published_at AT TIME ZONE 'UTC'
                ) BETWEEN 1 AND 15
            )
        GROUP BY
            skz_snippets.title,
            skz_snippets.published_at
        LIMIT 24;
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            data = result.mappings().all()
    except Exception:
        logger.error("Your data cannot be found.")
        data = None

    print(data)


if __name__ == "__main__":
    app()


# if __name__ == "__main__":

#     typer.run(main)
