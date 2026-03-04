from typing import cast

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from .config import Settings
from .google_api_protocols import DriveService, SheetsService

_GOOGLE_SCOPES = (
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
)


def _build_credentials(settings: Settings) -> Credentials:
    return Credentials.from_service_account_file(
        str(settings.google_service_account_credentials_file),
        scopes=list(_GOOGLE_SCOPES),
    )


def build_drive_client(settings: Settings) -> DriveService:
    credentials = _build_credentials(settings)
    client = build("drive", "v3", credentials=credentials, cache_discovery=False)
    return cast(DriveService, client)


def build_sheets_client(settings: Settings) -> SheetsService:
    credentials = _build_credentials(settings)
    client = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    return cast(SheetsService, client)
