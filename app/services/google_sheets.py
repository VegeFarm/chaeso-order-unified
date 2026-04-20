
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
import re
from typing import Any
from zoneinfo import ZoneInfo

from gspread.exceptions import WorksheetNotFound

from app.services.business_dates import resolve_auto_sheet_date
from app.services.rules_loader import load_item_settings
from app.services.sheets_client import open_spreadsheet
from app.utils.text import normalize_name

ITEM_HEADER_ALIASES = ["변환품명", "품목명", "품목"]
QTY_HEADER_ALIASES = ["입고수량", "입고", "수량"]
STOCK_HEADER_ALIASES = ["재고", "재고수량", "재고 수량"]
REMAINING_HEADER_ALIASES = ["남은수량", "남은 수량", "잔여수량", "잔여 수량", "남은재고"]
AGE_TRACKER_SHEET_NAME = "재고나이집계"
AGE_TRACKER_HEADERS = ["품목", "4일이상", "3일전", "2일전", "1일전"]
AGE_TRACKER_META_KEY = "STATE_FOR_DATE"


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


def _parse_sheet_number_preserve_value(value: Any) -> int | float:
    number = Decimal(str(value or 0).replace(",", ""))
    return int(number) if number == number.to_integral_value() else float(number)


def _normalize_receipt_unit(unit: str) -> str:
    normalized = normalize_name(unit).upper()
    if normalized in {"EA", "개"}:
        return "EA"
    if normalized in {"BOX", "박스"}:
        return "BOX"
    if normalized in {"PACK", "팩"}:
        return "팩"
    if normalized == "KG":
        return "KG"
    return normalized


def _build_transformed_qty_map(receipt_rows: list[dict[str, Any]], item_settings: dict[str, dict[str, Any]]) -> dict[str, float]:
    qty_map: dict[str, float] = defaultdict(float)
    for row in receipt_rows:
        original = str(row["name"])
        original_key = normalize_name(original)
        item_config = item_settings.get(original_key, {})
        transformed = str(item_config.get("transformed_name") or original).strip()

        unit = _normalize_receipt_unit(str(row.get("unit", "")))
        unit_multipliers = item_config.get("unit_multipliers", {}) or {}
        multiplier = float(unit_multipliers.get(unit, item_config.get("default_multiplier", 1.0) or 1.0))

        qty_map[transformed] += float(row["quantity"]) * multiplier
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


def _list_dated_worksheets(spreadsheet, settings, reference_date: date, exclude_titles: set[str] | None = None):
    ignored_titles = {
        settings.template_sheet_name,
        settings.item_settings_sheet_name,
        settings.match_rules_sheet_name,
        settings.price_rules_sheet_name,
        AGE_TRACKER_SHEET_NAME,
    }
    if exclude_titles:
        ignored_titles.update(exclude_titles)

    candidates: list[tuple[date, Any]] = []
    for worksheet in spreadsheet.worksheets():
        if worksheet.title in ignored_titles:
            continue
        parsed_date = _parse_sheet_title_date(worksheet.title, reference_date)
        if parsed_date is None:
            continue
        candidates.append((parsed_date, worksheet))

    candidates.sort(key=lambda item: item[0])
    return candidates


def _find_latest_previous_sheet(spreadsheet, settings, target_sheet_title: str, target_date_text: str):
    target_date = _parse_target_date(target_date_text)
    candidates = _list_dated_worksheets(spreadsheet, settings, target_date, exclude_titles={target_sheet_title})
    previous = [(parsed_date, ws) for parsed_date, ws in candidates if parsed_date < target_date]
    return previous[-1][1] if previous else None


def _build_item_value_map(worksheet, item_aliases: list[str], value_aliases: list[str]) -> dict[str, int | float]:
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

    item_value_map: dict[str, int | float] = {}
    for row in values[1:]:
        item_name = row[item_col - 1].strip() if len(row) >= item_col else ""
        if not item_name:
            continue

        raw_value = row[value_col - 1].strip() if len(row) >= value_col else ""
        if not raw_value:
            item_value_map[item_name] = 0
            continue

        try:
            parsed_value = _parse_sheet_number_preserve_value(raw_value)
            item_value_map[item_name] = max(0, parsed_value)
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


def _write_qty_only_to_template_layout(worksheet, receipt_rows: list[dict[str, Any]], item_settings: dict[str, dict[str, Any]]):
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

    qty_map = _build_transformed_qty_map(receipt_rows, item_settings)
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


def _to_float(value: Any) -> float:
    try:
        return max(0.0, float(value))
    except Exception:
        return 0.0


def _fmt_md(value: date) -> str:
    return f"{value.month}.{value.day:02d}"


def _get_or_create_age_tracker_sheet(spreadsheet):
    try:
        worksheet = spreadsheet.worksheet(AGE_TRACKER_SHEET_NAME)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=AGE_TRACKER_SHEET_NAME, rows=200, cols=8)
        worksheet.update("A1:E1", [AGE_TRACKER_HEADERS])
        worksheet.update("G1:H2", [["META_KEY", "META_VALUE"], [AGE_TRACKER_META_KEY, ""]])
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": worksheet.id, "hidden": True},
                            "fields": "hidden",
                        }
                    }
                ]
            }
        )
    return worksheet


def _read_age_tracker_state(worksheet) -> tuple[str | None, dict[str, list[float]]]:
    values = worksheet.get_all_values()
    state: dict[str, list[float]] = {}
    state_for_date: str | None = None

    for row in values[1:]:
        if not row:
            continue
        if row[0] == AGE_TRACKER_META_KEY:
            state_for_date = row[1].strip() if len(row) >= 2 and row[1].strip() else None
            continue
        item_name = row[0].strip()
        if not item_name:
            continue
        state[item_name] = [
            _to_float(row[1] if len(row) >= 2 else 0),
            _to_float(row[2] if len(row) >= 3 else 0),
            _to_float(row[3] if len(row) >= 4 else 0),
            _to_float(row[4] if len(row) >= 5 else 0),
        ]
    return state_for_date, state


def _write_age_tracker_state(worksheet, business_date: str, state: dict[str, list[float]]):
    rows = [AGE_TRACKER_HEADERS, [AGE_TRACKER_META_KEY, business_date, "", "", ""]]
    for item_name in sorted(state.keys()):
        buckets = state[item_name]
        rows.append([item_name] + [_coerce_number(v) for v in buckets])

    target_rows = max(len(rows), 2)
    target_range = f"A1:E{target_rows}"
    worksheet.batch_clear(["A1:E1000", "G1:H10"])
    worksheet.update(target_range, rows)
    worksheet.update("G1:H2", [["META_KEY", "META_VALUE"], [AGE_TRACKER_META_KEY, business_date]])


def _allocate_fifo_remaining(oldest_to_newest: list[float], final_total: float) -> list[float]:
    remaining_total = max(0.0, final_total)
    result = [0.0] * len(oldest_to_newest)
    for index in range(len(oldest_to_newest) - 1, -1, -1):
        keep = min(oldest_to_newest[index], remaining_total)
        result[index] = keep
        remaining_total -= keep
    return result


def _roll_forward_age_state(
    start_state: dict[str, list[float]],
    intake_map: dict[str, int | float],
    remaining_map: dict[str, int | float],
) -> dict[str, list[float]]:
    next_state: dict[str, list[float]] = {}
    item_names = set(start_state.keys()) | set(intake_map.keys()) | set(remaining_map.keys())

    for item_name in item_names:
        old_plus, day3, day2, day1 = start_state.get(item_name, [0.0, 0.0, 0.0, 0.0])
        intake = _to_float(intake_map.get(item_name, 0))
        final_total = _to_float(remaining_map.get(item_name, 0))
        available = [old_plus, day3, day2, day1, intake]
        kept = _allocate_fifo_remaining(available, final_total)
        new_state = [kept[0] + kept[1], kept[2], kept[3], kept[4]]
        if any(value > 0 for value in new_state):
            next_state[item_name] = new_state

    return next_state


def _bootstrap_age_state_for_target(spreadsheet, settings, target_date: date) -> tuple[dict[str, list[float]], list[str]]:
    dated_sheets = _list_dated_worksheets(spreadsheet, settings, target_date, exclude_titles=set())
    previous_sheets = [(sheet_date, ws) for sheet_date, ws in dated_sheets if sheet_date < target_date]

    state: dict[str, list[float]] = {}
    sources: list[str] = []
    for _, ws in previous_sheets:
        intake_map = _build_item_value_map(ws, ITEM_HEADER_ALIASES, QTY_HEADER_ALIASES)
        remaining_map = _build_item_value_map(ws, ITEM_HEADER_ALIASES, REMAINING_HEADER_ALIASES)
        state = _roll_forward_age_state(state, intake_map, remaining_map)
        sources.append(ws.title)

    return state, sources


def _build_or_get_age_state_for_target(spreadsheet, settings, target_date: date):
    tracker_ws = _get_or_create_age_tracker_sheet(spreadsheet)
    state_for_date, tracked_state = _read_age_tracker_state(tracker_ws)
    business_date = target_date.strftime("%Y-%m-%d")
    if state_for_date == business_date:
        return tracker_ws, tracked_state, "tracker_reused", []

    latest_previous_ws = _find_latest_previous_sheet(spreadsheet, settings, f"{target_date.month}.{target_date.day:02d}", business_date)
    if latest_previous_ws is None:
        state: dict[str, list[float]] = {}
        source_titles: list[str] = []
        source_mode = "empty"
    else:
        previous_date = _parse_sheet_title_date(latest_previous_ws.title, target_date)
        previous_business_date = previous_date.strftime("%Y-%m-%d") if previous_date else None
        if state_for_date == previous_business_date and tracked_state:
            intake_map = _build_item_value_map(latest_previous_ws, ITEM_HEADER_ALIASES, QTY_HEADER_ALIASES)
            remaining_map = _build_item_value_map(latest_previous_ws, ITEM_HEADER_ALIASES, REMAINING_HEADER_ALIASES)
            state = _roll_forward_age_state(tracked_state, intake_map, remaining_map)
            source_titles = [latest_previous_ws.title]
            source_mode = "rolled_from_tracker"
        else:
            state, source_titles = _bootstrap_age_state_for_target(spreadsheet, settings, target_date)
            source_mode = "bootstrapped_from_recent_sheets"

    _write_age_tracker_state(tracker_ws, business_date, state)
    return tracker_ws, state, source_mode, source_titles


def _build_stock_note(target_date: date, buckets: list[float]) -> str:
    old_plus, day3, day2, day1 = buckets
    total = sum(buckets)
    if total <= 0:
        return ""

    lines = [f"재고 구성 {int(total) if float(total).is_integer() else total}"]
    labels = [
        (day1, f"- {_fmt_md(target_date - timedelta(days=1))} 입고분"),
        (day2, f"- {_fmt_md(target_date - timedelta(days=2))} 입고분"),
        (day3, f"- {_fmt_md(target_date - timedelta(days=3))} 입고분"),
        (old_plus, "- 4일 이상"),
    ]
    for qty, label in labels:
        if qty <= 0:
            continue
        lines.append(f"{label}: {_coerce_number(qty)}개")
    return "\n".join(lines)


def _apply_stock_notes(worksheet, target_date: date, state: dict[str, list[float]]) -> int:
    headers = worksheet.row_values(1)
    item_col = _first_header_index(headers, ITEM_HEADER_ALIASES)
    stock_col = _first_header_index(headers, STOCK_HEADER_ALIASES)
    if item_col is None or stock_col is None:
        return 0

    all_values = worksheet.get_all_values()
    if len(all_values) <= 1:
        return 0

    requests: list[dict[str, Any]] = []
    updated = 0
    for row_index, row in enumerate(all_values[1:], start=2):
        item_name = row[item_col - 1].strip() if len(row) >= item_col else ""
        if not item_name:
            continue
        note = _build_stock_note(target_date, state.get(item_name, [0.0, 0.0, 0.0, 0.0]))
        requests.append(
            {
                "updateCells": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": row_index - 1,
                        "endRowIndex": row_index,
                        "startColumnIndex": stock_col - 1,
                        "endColumnIndex": stock_col,
                    },
                    "rows": [{"values": [{"note": note}]}],
                    "fields": "note",
                }
            }
        )
        updated += 1

    if requests:
        worksheet.spreadsheet.batch_update({"requests": requests})
    return updated


def _delete_old_date_sheets(spreadsheet, settings, target_date: date) -> list[str]:
    cutoff = target_date - timedelta(days=3)
    deleted_titles: list[str] = []
    for sheet_date, worksheet in _list_dated_worksheets(spreadsheet, settings, target_date, exclude_titles={f"{target_date.month}.{target_date.day:02d}"}):
        if sheet_date < cutoff:
            spreadsheet.del_worksheet(worksheet)
            deleted_titles.append(worksheet.title)
    return deleted_titles


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
    _, age_state, age_state_mode, age_state_sources = _build_or_get_age_state_for_target(
        spreadsheet,
        settings,
        target_date,
    )
    stock_note_updated_items = _apply_stock_notes(worksheet, target_date, age_state)
    deleted_old_sheets = _delete_old_date_sheets(spreadsheet, settings, target_date)

    return {
        "sheet_title": worksheet.title,
        "business_date": business_date,
        "create_mode": create_mode,
        "auto_sheet_weekday": now.strftime("%A"),
        "auto_sheet_target_date": business_date,
        "auto_sheet_saturday_skipped": now.weekday() == 5,
        "age_tracker_sheet": AGE_TRACKER_SHEET_NAME,
        "age_state_mode": age_state_mode,
        "age_state_source_sheets": age_state_sources,
        "stock_note_updated_items": stock_note_updated_items,
        "deleted_old_sheets": deleted_old_sheets,
        **seed_result,
    }


def update_daily_sheet(settings, sheet_title: str, business_date: str, receipt_rows: list[dict[str, Any]]):
    spreadsheet = open_spreadsheet(settings)
    item_settings = load_item_settings(settings)
    worksheet, create_mode, seed_result = _get_or_create_target_sheet(spreadsheet, settings, sheet_title, business_date)
    result = _write_qty_only_to_template_layout(worksheet, receipt_rows, item_settings)
    result["create_mode"] = create_mode
    result["business_date"] = business_date
    result.update(seed_result)
    return result
