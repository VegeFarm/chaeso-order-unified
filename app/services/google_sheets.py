from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
import re
from typing import Any
from zoneinfo import ZoneInfo

from gspread.exceptions import WorksheetNotFound

from app.services.business_dates import resolve_auto_sheet_date
from app.services.rules_loader import load_item_name_map
from app.services.sheets_client import open_spreadsheet
from app.utils.text import normalize_name

ITEM_HEADER_ALIASES = ["변환품명", "품목명", "품목"]
QTY_HEADER_ALIASES = ["입고수량", "입고", "수량"]
STOCK_HEADER_ALIASES = ["재고", "재고수량", "재고 수량"]
REMAINING_HEADER_ALIASES = ["남은수량", "남은 수량", "잔여수량", "잔여 수량", "남은재고"]


class SheetUpdateError(Exception):
    pass


def _first_header_index(headers: list[str], aliases: list[str]) -> int | None:
    for alias in aliases:
        if alias in headers:
            return headers.index(alias) + 1
    return None


def _coerce_number(value: float) -> int | float:
    value = float(value)
    return int(round(value)) if value.is_integer() else value


def _round_half_up_to_int(value: Any) -> int:
    number = Decimal(str(value or 0))
    return int(number.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _build_transformed_qty_map(receipt_rows: list[dict[str, Any]], name_map: dict[str, str]) -> dict[str, float]:
    qty_map: dict[str, float] = defaultdict(float)
    for row in receipt_rows:
        original = str(row["name"])
        transformed = name_map.get(normalize_name(original), original)
        qty_map[transformed] += float(row["quantity"])
    return dict(qty_map)


def _duplicate_template_sheet(spreadsheet, template_title: str, sheet_title: str):
    template_ws = spreadsheet.worksheet(template_title)
    spreadsheet.batch_update(
        {
            "requests": [
                {
                    "duplicateSheet": {
                        "sourceSheetId": template_ws.id,
                        "newSheetName": sheet_title,
                    }
                }
            ]
        }
    )
    return spreadsheet.worksheet(sheet_title)


def _parse_target_date(target_date_text: str) -> date:
    digits = re.sub(r"[^0-9]", "", str(target_date_text))
    if len(digits) < 8:
        raise SheetUpdateError(f"날짜 형식을 해석하지 못했습니다: {target_date_text}")
    return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))


def _parse_sheet_title_date(sheet_title: str, reference_date: date) -> date | None:
    title = str(sheet_title).strip()

    full_match = re.fullmatch(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})", title)
    if full_match:
        year, month, day = map(int, full_match.groups())
        try:
            return date(year, month, day)
        except ValueError:
            return None

    short_match = re.fullmatch(r"(\d{1,2})[./-](\d{1,2})", title)
    if short_match:
        month, day = map(int, short_match.groups())
        year = reference_date.year
        try:
            candidate = date(year, month, day)
        except ValueError:
            return None
        if candidate > reference_date:
            try:
                candidate = date(year - 1, month, day)
            except ValueError:
                return None
        return candidate

    return None


def _find_latest_previous_sheet(spreadsheet, settings, target_sheet_title: str, target_date_text: str):
    target_date = _parse_target_date(target_date_text)
    ignored_titles = {
        settings.template_sheet_name,
        settings.item_settings_sheet_name,
        settings.match_rules_sheet_name,
        settings.price_rules_sheet_name,
        target_sheet_title,
    }

    candidates: list[tuple[date, Any]] = []
    for worksheet in spreadsheet.worksheets():
        if worksheet.title in ignored_titles:
            continue
        parsed_date = _parse_sheet_title_date(worksheet.title, target_date)
        if parsed_date is None:
            continue
        if parsed_date >= target_date:
            continue
        candidates.append((parsed_date, worksheet))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def _build_item_value_map(worksheet, item_aliases: list[str], value_aliases: list[str]) -> dict[str, int]:
    headers = worksheet.row_values(1)
    if not headers:
        return {}

    item_col = _first_header_index(headers, item_aliases)
    value_col = _first_header_index(headers, value_aliases)
    if item_col is None or value_col is None:
        return {}

    values = worksheet.get_all_values()
    if len(values) <= 1:
        return {}

    item_value_map: dict[str, int] = {}
    for row in values[1:]:
        item_name = row[item_col - 1].strip() if len(row) >= item_col else ""
        if not item_name:
            continue

        raw_value = row[value_col - 1].strip() if len(row) >= value_col else ""
        if not raw_value:
            item_value_map[item_name] = 0
            continue

        cleaned = raw_value.replace(",", "")
        try:
            item_value_map[item_name] = _round_half_up_to_int(cleaned)
        except Exception:
            item_value_map[item_name] = 0

    return item_value_map


def _copy_latest_remaining_to_stock(spreadsheet, settings, worksheet, sheet_title: str, business_date: str) -> dict[str, Any]:
    latest_ws = _find_latest_previous_sheet(spreadsheet, settings, sheet_title, business_date)
    if latest_ws is None:
        return {
            "stock_seeded": False,
            "stock_seed_source_sheet": None,
            "stock_seed_updated_items": 0,
            "stock_seed_reason": "previous_sheet_not_found",
        }

    target_headers = worksheet.row_values(1)
    item_col = _first_header_index(target_headers, ITEM_HEADER_ALIASES)
    stock_col = _first_header_index(target_headers, STOCK_HEADER_ALIASES)
    if item_col is None or stock_col is None:
        return {
            "stock_seeded": False,
            "stock_seed_source_sheet": latest_ws.title,
            "stock_seed_updated_items": 0,
            "stock_seed_reason": "target_sheet_missing_headers",
        }

    previous_item_value_map = _build_item_value_map(latest_ws, ITEM_HEADER_ALIASES, REMAINING_HEADER_ALIASES)
    if not previous_item_value_map:
        return {
            "stock_seeded": False,
            "stock_seed_source_sheet": latest_ws.title,
            "stock_seed_updated_items": 0,
            "stock_seed_reason": "source_sheet_missing_headers_or_values",
        }

    all_values = worksheet.get_all_values()
    if len(all_values) <= 1:
        return {
            "stock_seeded": False,
            "stock_seed_source_sheet": latest_ws.title,
            "stock_seed_updated_items": 0,
            "stock_seed_reason": "target_sheet_has_no_rows",
        }

    updated_count = 0
    for row_index, row in enumerate(all_values[1:], start=2):
        item_name = row[item_col - 1].strip() if len(row) >= item_col else ""
        if not item_name:
            continue
        stock_value = previous_item_value_map.get(item_name, 0)
        worksheet.update_cell(row_index, stock_col, stock_value)
        updated_count += 1

    return {
        "stock_seeded": True,
        "stock_seed_source_sheet": latest_ws.title,
        "stock_seed_updated_items": updated_count,
        "stock_seed_reason": None,
    }


def _get_or_create_target_sheet(spreadsheet, settings, sheet_title: str, business_date: str):
    try:
        return spreadsheet.worksheet(sheet_title), "existing_sheet", {}
    except WorksheetNotFound:
        try:
            worksheet = _duplicate_template_sheet(spreadsheet, settings.template_sheet_name, sheet_title)
            seed_result = _copy_latest_remaining_to_stock(
                spreadsheet,
                settings,
                worksheet,
                sheet_title,
                business_date,
            )
            return worksheet, "created_from_template", seed_result
        except WorksheetNotFound as exc:
            raise SheetUpdateError(
                f"'{settings.template_sheet_name}' 템플릿 시트를 찾지 못했습니다. "
                "구글시트에 템플릿 시트를 먼저 만들어 주세요."
            ) from exc


def _write_qty_only_to_template_layout(worksheet, receipt_rows: list[dict[str, Any]], name_map: dict[str, str]):
    headers = worksheet.row_values(1)
    if not headers:
        raise SheetUpdateError("템플릿 시트 1행에서 헤더를 찾지 못했습니다.")

    item_col = _first_header_index(headers, ITEM_HEADER_ALIASES)
    qty_col = _first_header_index(headers, QTY_HEADER_ALIASES)

    if item_col is None or qty_col is None:
        raise SheetUpdateError("템플릿 시트에서 '품목' 열과 '입고' 열을 찾지 못했습니다.")

    all_values = worksheet.get_all_values()
    if len(all_values) <= 1:
        raise SheetUpdateError("템플릿 시트에 품목 데이터가 없습니다.")

    qty_map = _build_transformed_qty_map(receipt_rows, name_map)
    data_rows = all_values[1:]

    existing_index_by_item: dict[str, int] = {}
    item_rows: list[int] = []

    for row_index, row in enumerate(data_rows, start=2):
        current_item = row[item_col - 1].strip() if len(row) >= item_col else ""
        if current_item:
            existing_index_by_item[current_item] = row_index
            item_rows.append(row_index)

    if not item_rows:
        raise SheetUpdateError("템플릿 시트에 품목 행이 없습니다.")

    for row_index in item_rows:
        worksheet.update_cell(row_index, qty_col, 0)

    matched_count = 0
    skipped_items: list[str] = []

    for transformed_name, qty in qty_map.items():
        target_row = existing_index_by_item.get(transformed_name)
        if target_row is None:
            skipped_items.append(transformed_name)
            continue

        worksheet.update_cell(target_row, qty_col, _coerce_number(qty))
        matched_count += 1

    return {
        "sheet_title": worksheet.title,
        "mode": "qty_only_template_layout",
        "updated_items": matched_count,
        "skipped_items": skipped_items,
    }


def ensure_today_sheet(settings) -> dict[str, Any]:
    now = datetime.now(ZoneInfo(settings.business_timezone))
    target_date = resolve_auto_sheet_date(now)
    business_date = target_date.strftime("%Y-%m-%d")
    sheet_title = f"{target_date.month}.{target_date.day:02d}"

    spreadsheet = open_spreadsheet(settings)
    worksheet, create_mode, seed_result = _get_or_create_target_sheet(
        spreadsheet,
        settings,
        sheet_title,
        business_date,
    )
    return {
        "sheet_title": worksheet.title,
        "business_date": business_date,
        "create_mode": create_mode,
        "auto_sheet_weekday": now.strftime("%A"),
        "auto_sheet_target_date": business_date,
        "auto_sheet_saturday_skipped": now.weekday() == 5,
        **seed_result,
    }


def update_daily_sheet(settings, sheet_title: str, business_date: str, receipt_rows: list[dict[str, Any]]):
    spreadsheet = open_spreadsheet(settings)
    name_map = load_item_name_map(settings)
    worksheet, create_mode, seed_result = _get_or_create_target_sheet(spreadsheet, settings, sheet_title, business_date)
    result = _write_qty_only_to_template_layout(worksheet, receipt_rows, name_map)
    result["create_mode"] = create_mode
    result["business_date"] = business_date
    result.update(seed_result)
    return result
