from collections import defaultdict
from typing import Any

from gspread.exceptions import WorksheetNotFound

from app.services.rules_loader import load_item_name_map
from app.services.sheets_client import open_spreadsheet
from app.utils.text import normalize_name

ITEM_HEADER_ALIASES = ["변환품명", "품목명", "품목"]
QTY_HEADER_ALIASES = ["입고수량", "입고", "수량"]


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


def _get_or_create_target_sheet(spreadsheet, settings, sheet_title: str):
    try:
        return spreadsheet.worksheet(sheet_title), "existing_sheet"
    except WorksheetNotFound:
        try:
            worksheet = _duplicate_template_sheet(spreadsheet, settings.template_sheet_name, sheet_title)
            return worksheet, "created_from_template"
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

    # 먼저 기존 입고값을 전부 0으로 초기화해서 이전 실행 흔적이 남지 않게 함
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


def update_daily_sheet(settings, sheet_title: str, business_date: str, receipt_rows: list[dict[str, Any]]):
    spreadsheet = open_spreadsheet(settings)
    name_map = load_item_name_map(settings)
    worksheet, create_mode = _get_or_create_target_sheet(spreadsheet, settings, sheet_title)
    result = _write_qty_only_to_template_layout(worksheet, receipt_rows, name_map)
    result["create_mode"] = create_mode
    result["business_date"] = business_date
    return result
