import gspread
from google.oauth2.service_account import Credentials

from app.config import Settings

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_gspread_client(settings: Settings) -> gspread.Client:
    credentials = Credentials.from_service_account_info(
        settings.google_service_account_info,
        scopes=SCOPES,
    )
    return gspread.authorize(credentials)


def open_spreadsheet(settings: Settings):
    client = get_gspread_client(settings)
    return client.open_by_key(settings.spreadsheet_id)
