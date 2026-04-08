import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load variables from .env into the environment
load_dotenv()

db_uri = os.environ.get("LOCAL_DB_URL")

if not db_uri:
    raise ValueError(
        "LOCAL_DB_URL is missing! Double-check that your .env file exists and is formatted correctly."
    )

try:
    # Attempt to connect to the database
    engine = create_engine(db_uri)
    with engine.connect() as connection:
        print("Success! Python is connected to your local PostgreSQL database.")
except Exception as e:
    print(f"Connection failed: {e}")
