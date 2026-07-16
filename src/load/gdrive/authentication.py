"""Manage Google Drive API authentication and service initialization."""

import json
import os
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from loguru import logger


def get_drive_service(gcp_credentials_key: str = "GCP_CREDENTIALS") -> Any:
    """Authenticate and return the Google Drive API service client.

    Args:
        gcp_credentials_key (str, optional): The environment variable key containing
            the JSON-formatted GCP service account credentials. Defaults to "GCP_CREDENTIALS".

    Returns:
        Any: The authenticated Google Drive API service instance, or None if the
        credentials are missing from the environment.
    """
    creds_json = os.environ.get(gcp_credentials_key, "")

    if not creds_json:
        logger.error(f"Missing '{gcp_credentials_key}' key from environment.")
        return

    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/drive"]
    )

    return build("drive", "v3", credentials=credentials)
