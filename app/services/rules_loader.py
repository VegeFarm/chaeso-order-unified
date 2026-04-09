from typing import Any

from app.services.sheets_client import open_spreadsheet
from app.utils.text import normalize_name

ACTIVE_VALUES = {"y", "yes", "1", "true", "사용"}


def _is_active(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in ACTIVE_VALUES


def _worksheet_records(spreadsheet, title: str) -> list[dict[str, Any]]:
    worksheet = spreadsheet.worksheet(title)
    records = worksheet.get_all_records(default_blank="")
    return [row for row in records if any(str(v).strip() for v in row.values())]


def load_match_rules(settings) -> dict[str, dict[str, Any]]:
    spreadsheet = open_spreadsheet(settings)
    rows = _worksheet_records(spreadsheet, settings.match_rules_sheet_name)

    rules: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not _is_active(row.get("사용여부", "")):
            continue

        display_name = str(row.get("변환품명", "")).strip()
        original_name = str(row.get("원본품명", "")).strip()
        multiplier_raw = row.get("수량배수", "")

        if not display_name or not original_name or multiplier_raw == "":
            continue

        rules[display_name] = {
            "keyword": original_name,
            "multiplier": float(multiplier_raw),
        }

    return rules


def load_price_rules(settings) -> dict[str, dict[str, Any]]:
    spreadsheet = open_spreadsheet(settings)
    rows = _worksheet_records(spreadsheet, settings.price_rules_sheet_name)

    rules: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not _is_active(row.get("사용여부", "")):
            continue

        display_name = str(row.get("변환품명", "")).strip()
        original_name = str(row.get("원본품명", "")).strip()
        units_raw = row.get("단위수량", "")
        round_to_raw = row.get("반올림단위", "")

        if not display_name or not original_name or units_raw == "" or round_to_raw == "":
            continue

        rules[display_name] = {
            "keyword": original_name,
            "units_per_order": float(units_raw),
            "round_to": int(float(round_to_raw)),
        }

    return rules


def load_item_name_map(settings) -> dict[str, str]:
    spreadsheet = open_spreadsheet(settings)
    rows = _worksheet_records(spreadsheet, settings.item_settings_sheet_name)

    name_map: dict[str, str] = {}
    for row in rows:
        active_value = row.get("사용여부", "Y")
        if str(active_value).strip() and not _is_active(active_value):
            continue

        original_name = str(row.get("원본품명", "")).strip()
        transformed_name = str(row.get("변환품명", "")).strip()
        if not original_name or not transformed_name:
            continue

        name_map[normalize_name(original_name)] = transformed_name

    return name_map
