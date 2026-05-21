import json
import os
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from loguru import logger


def get_drive_service(
    gcp_credentials_key: str = "GCP_SA_CREDENTIALS", *, return_bool: bool = False
) -> Any:
    """Authenticate and return the Google Drive API service."""
    # Load credentials from the environment variable
    creds_json = os.environ.get(gcp_credentials_key, "")

    if not creds_json:
        logger.error(f"Missing '{gcp_credentials_key}' key from environment.")
        return False if return_bool else None

    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/drive"]
    )

    return build("drive", "v3", credentials=credentials)
