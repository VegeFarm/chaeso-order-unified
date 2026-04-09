from collections import defaultdict
from typing import Any

from gspread.exceptions import WorksheetNotFound

from app.services.rules_loader import load_item_name_map
from app.services.sheets_client import open_spreadsheet
from app.utils.text import format_plain_qty, normalize_name

ITEM_HEADER_ALIASES = ["변환품명", "품목명", "품목"]
QTY_HEADER_ALIASES = ["입고수량", "입고", "수량"]
ORIGINAL_HEADER_ALIASES = ["원본품명"]
UNIT_HEADER_ALIASES = ["단위"]
UNIT_PRICE_HEADER_ALIASES = ["단가"]
SUM_HEADER_ALIASES = ["금액", "합계금액", "sum_amount"]
DATE_HEADER_ALIASES = ["영업일자", "일자", "날짜"]


class SheetUpdateError(Exception):
    pass


def _first_header_index(headers: list[str], aliases: list[str]) -> int | None:
    for alias in aliases:
        if alias in headers:
            return headers.index(alias) + 1
    return None


def _ensure_default_headers(worksheet):
    if worksheet.row_count < 1:
        worksheet.add_rows(1)
    default_headers = ["영업일자", "원본품명", "변환품명", "입고수량", "단위", "단가", "금액"]
    worksheet.update("A1:G1", [default_headers])
    return default_headers


def _prepare_standard_rows(business_date: str, receipt_rows: list[dict[str, Any]], name_map: dict[str, str]):
    aggregated: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(lambda: {
        "quantity": 0.0,
        "unit_price": 0.0,
        "sum_amount": 0.0,
    })

    for row in receipt_rows:
        original = row["name"]
        transformed = name_map.get(normalize_name(original), original)
        key = (business_date, original, transformed)
        aggregated[key]["quantity"] += float(row["quantity"])
        aggregated[key]["unit_price"] = float(row["unit_price"])
        aggregated[key]["sum_amount"] += float(row["sum_amount"])
        aggregated[key]["unit"] = row["unit"]

    values: list[list[str]] = []
    for (date_value, original, transformed), payload in aggregated.items():
        values.append(
            [
                date_value,
                original,
                transformed,
                format_plain_qty(payload["quantity"]),
                str(payload.get("unit", "")),
                format_plain_qty(payload["unit_price"]),
                format_plain_qty(payload["sum_amount"]),
            ]
        )
    return values


def _upsert_into_existing_layout(worksheet, business_date: str, receipt_rows: list[dict[str, Any]], name_map: dict[str, str]):
    headers = worksheet.row_values(1)
    if not headers:
        headers = _ensure_default_headers(worksheet)

    item_col = _first_header_index(headers, ITEM_HEADER_ALIASES)
    qty_col = _first_header_index(headers, QTY_HEADER_ALIASES)
    original_col = _first_header_index(headers, ORIGINAL_HEADER_ALIASES)
    unit_col = _first_header_index(headers, UNIT_HEADER_ALIASES)
    unit_price_col = _first_header_index(headers, UNIT_PRICE_HEADER_ALIASES)
    sum_col = _first_header_index(headers, SUM_HEADER_ALIASES)
    date_col = _first_header_index(headers, DATE_HEADER_ALIASES)

    if item_col is None or qty_col is None:
        worksheet.clear()
        headers = _ensure_default_headers(worksheet)
        standard_rows = _prepare_standard_rows(business_date, receipt_rows, name_map)
        if standard_rows:
            worksheet.update(f"A2:G{len(standard_rows)+1}", standard_rows)
        return {
            "sheet_title": worksheet.title,
            "mode": "recreated_standard_layout",
            "updated_items": len(standard_rows),
        }

    existing_values = worksheet.get_all_values()
    data_rows = existing_values[1:] if len(existing_values) > 1 else []

    transformed_payload: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "quantity": 0.0,
        "original_names": set(),
        "unit": "",
        "unit_price": 0.0,
        "sum_amount": 0.0,
    })
    for row in receipt_rows:
        original = row["name"]
        transformed = name_map.get(normalize_name(original), original)
        transformed_payload[transformed]["quantity"] += float(row["quantity"])
        transformed_payload[transformed]["original_names"].add(original)
        transformed_payload[transformed]["unit"] = row["unit"]
        transformed_payload[transformed]["unit_price"] = float(row["unit_price"])
        transformed_payload[transformed]["sum_amount"] += float(row["sum_amount"])

    existing_index_by_item: dict[str, int] = {}
    for idx, row in enumerate(data_rows, start=2):
        current_item = row[item_col - 1].strip() if len(row) >= item_col else ""
        if current_item:
            existing_index_by_item[current_item] = idx

    next_row = len(existing_values) + 1 if existing_values else 2
    updates = []
    appended = []

    for transformed_name, payload in transformed_payload.items():
        target_row = existing_index_by_item.get(transformed_name)
        if target_row:
            updates.append((target_row, qty_col, format_plain_qty(payload["quantity"])))
            if original_col:
                updates.append((target_row, original_col, ", ".join(sorted(payload["original_names"]))))
            if unit_col:
                updates.append((target_row, unit_col, str(payload["unit"])))
            if unit_price_col:
                updates.append((target_row, unit_price_col, format_plain_qty(payload["unit_price"])))
            if sum_col:
                updates.append((target_row, sum_col, format_plain_qty(payload["sum_amount"])))
            if date_col:
                updates.append((target_row, date_col, business_date))
        else:
            row_buffer = [""] * max(len(headers), 7)
            row_buffer[item_col - 1] = transformed_name
            row_buffer[qty_col - 1] = format_plain_qty(payload["quantity"])
            if original_col:
                row_buffer[original_col - 1] = ", ".join(sorted(payload["original_names"]))
            if unit_col:
                row_buffer[unit_col - 1] = str(payload["unit"])
            if unit_price_col:
                row_buffer[unit_price_col - 1] = format_plain_qty(payload["unit_price"])
            if sum_col:
                row_buffer[sum_col - 1] = format_plain_qty(payload["sum_amount"])
            if date_col:
                row_buffer[date_col - 1] = business_date
            appended.append(row_buffer)

    for row_index, col_index, value in updates:
        worksheet.update_cell(row_index, col_index, value)

    if appended:
        worksheet.append_rows(appended, value_input_option="USER_ENTERED")

    return {
        "sheet_title": worksheet.title,
        "mode": "updated_existing_layout",
        "updated_items": len(transformed_payload),
    }


def update_daily_sheet(settings, sheet_title: str, business_date: str, receipt_rows: list[dict[str, Any]]):
    spreadsheet = open_spreadsheet(settings)
    name_map = load_item_name_map(settings)

    try:
        worksheet = spreadsheet.worksheet(sheet_title)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_title, rows=300, cols=10)
        _ensure_default_headers(worksheet)

    return _upsert_into_existing_layout(worksheet, business_date, receipt_rows, name_map)
