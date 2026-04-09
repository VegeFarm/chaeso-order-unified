import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    spreadsheet_id: str
    google_service_account_info: dict
    item_settings_sheet_name: str
    match_rules_sheet_name: str
    price_rules_sheet_name: str
    template_sheet_name: str
    telegram_bot_token: str
    telegram_chat_id: str
    max_upload_mb: int


def _load_service_account_info() -> dict:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 환경변수가 비어 있습니다.")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 값이 올바른 JSON이 아닙니다.") from exc


def get_settings() -> Settings:
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("SPREADSHEET_ID 환경변수가 비어 있습니다.")

    return Settings(
        spreadsheet_id=spreadsheet_id,
        google_service_account_info=_load_service_account_info(),
        item_settings_sheet_name=os.getenv("ITEM_SETTINGS_SHEET_NAME", "품목설정").strip() or "품목설정",
        match_rules_sheet_name=os.getenv("MATCH_RULES_SHEET_NAME", "주문대조설정").strip() or "주문대조설정",
        price_rules_sheet_name=os.getenv("PRICE_RULES_SHEET_NAME", "가격설정").strip() or "가격설정",
        template_sheet_name=os.getenv("TEMPLATE_SHEET_NAME", "템플릿").strip() or "템플릿",
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        max_upload_mb=int(os.getenv("MAX_UPLOAD_MB", "15")),
    )
