from typing import Any

from app.services.sheets_client import open_spreadsheet
from app.utils.text import normalize_name

ACTIVE_VALUES = {"y", "yes", "1", "true", "사용"}

ITEM_ORIGINAL_NAME_ALIASES = ["원본품명", "원본 품명", "원본명", "원본"]
ITEM_TRANSFORMED_NAME_ALIASES = ["변환품명", "변환 품명", "품목명", "품목"]
ITEM_DEFAULT_MULTIPLIER_ALIASES = ["수량배수", "기본배수", "기본 수량배수"]
ITEM_UNIT_MULTIPLIER_ALIASES = {
    "BOX": [
        "BOX", "box", "BOX수량", "BOX 수량", "BOX당수량", "BOX당 수량",
        "박스수량", "박스 수량", "박스당수량", "박스당 수량", "박스당개수", "박스당 개수",
    ],
    "팩": [
        "팩", "PACK", "pack", "팩수량", "팩 수량", "팩당수량", "팩당 수량", "팩당개수", "팩당 개수",
    ],
    "EA": [
        "EA", "ea", "개", "EA수량", "EA 수량", "EA당수량", "EA당 수량",
        "개수", "개 수", "개당수량", "개당 수량",
    ],
    "KG": ["KG", "kg", "KG수량", "KG 수량", "kg수량", "kg 수량"],
}


def _is_active(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in ACTIVE_VALUES


def _worksheet_records(spreadsheet, title: str) -> list[dict[str, Any]]:
    worksheet = spreadsheet.worksheet(title)
    records = worksheet.get_all_records(default_blank="")
    return [row for row in records if any(str(v).strip() for v in row.values())]


def _first_value(row: dict[str, Any], aliases: list[str]) -> Any:
    for alias in aliases:
        if alias in row and str(row.get(alias, "")).strip() != "":
            return row.get(alias)
    return ""


def _parse_float(value: Any, default: float = 1.0) -> float:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


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


def load_item_settings(settings) -> dict[str, dict[str, Any]]:
    spreadsheet = open_spreadsheet(settings)
    rows = _worksheet_records(spreadsheet, settings.item_settings_sheet_name)

    item_settings: dict[str, dict[str, Any]] = {}
    for row in rows:
        active_value = row.get("사용여부", "Y")
        if str(active_value).strip() and not _is_active(active_value):
            continue

        original_name = str(_first_value(row, ITEM_ORIGINAL_NAME_ALIASES)).strip()
        transformed_name = str(_first_value(row, ITEM_TRANSFORMED_NAME_ALIASES)).strip()
        if not original_name or not transformed_name:
            continue

        unit_multipliers: dict[str, float] = {}
        for unit, aliases in ITEM_UNIT_MULTIPLIER_ALIASES.items():
            raw_value = _first_value(row, aliases)
            if str(raw_value).strip() == "":
                continue
            unit_multipliers[unit] = _parse_float(raw_value, default=1.0)

        default_multiplier = _parse_float(_first_value(row, ITEM_DEFAULT_MULTIPLIER_ALIASES), default=1.0)

        item_settings[normalize_name(original_name)] = {
            "transformed_name": transformed_name,
            "unit_multipliers": unit_multipliers,
            "default_multiplier": default_multiplier,
        }

    return item_settings


def load_item_name_map(settings) -> dict[str, str]:
    item_settings = load_item_settings(settings)
    return {
        original_name: str(config.get("transformed_name", "")).strip()
        for original_name, config in item_settings.items()
        if str(config.get("transformed_name", "")).strip()
    }
