from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from gspread.exceptions import WorksheetNotFound

from app.config import Settings
from app.services.sheets_client import open_spreadsheet_by_id
from app.utils.text import normalize_name

PURCHASE_RECORD_SHEET = "매입기록"
CONVERSION_SHEET = "품목환산표"
UNIT_PRICE_SHEET = "원물단가표"
UNREGISTERED_SHEET = "미등록상품"
RENAME_SHEET = "상품명변경"
SLIP_HISTORY_SHEET = "전표관리"
SLIP_DETAIL_SHEET = "전표상세관리"
DROPDOWN_SHEET = "드롭다운목록"

PURCHASE_RECORD_HEADERS = ["매입일", "상품명", "매입량", "매입총액", "단가", "월"]
CONVERSION_HEADERS = ["원본품명", "변환품명", "단위", "1개당 환산수량", "사용여부"]
UNIT_PRICE_HEADERS = ["월", "매입일", "상품명", "매입량", "매입총액", "단가"]
UNREGISTERED_HEADERS = ["업로드일", "전표번호", "매입일", "원본품명", "단위", "수량", "금액", "사유", "처리상태"]
RENAME_HEADERS = ["기존상품명", "변경상품명", "오류내용"]
SLIP_HISTORY_HEADERS = ["전표번호", "매입일", "총금액", "입력완료금액", "미입력금액", "처리상태"]
SLIP_DETAIL_HEADERS = ["전표번호", "매입일", "원본품명", "변환품명", "단위", "수량", "금액", "처리상태"]
DROPDOWN_HEADERS = ["월목록", "상품명목록"]

ACTIVE_VALUES = {"", "y", "yes", "1", "true", "사용"}
INACTIVE_VALUES = {"n", "no", "0", "false", "미사용", "제외"}
UNRESOLVED_STATUSES = {"품목환산표 미등록", "환산수량 없음", "단위 불일치", "변환품명 없음"}
COMPLETE_STATUSES = {"입력완료", "미사용제외"}


class PurchaseRecordError(Exception):
    pass


@dataclass(frozen=True)
class ConversionRule:
    original_name: str
    converted_name: str
    unit: str
    multiplier: float
    active: bool


def _now_date(settings: Settings) -> str:
    return datetime.now(ZoneInfo(settings.business_timezone)).strftime("%Y-%m-%d")


def _normalize_unit(unit: Any) -> str:
    text = normalize_name(str(unit or "")).upper()
    if text in {"박스"}:
        return "BOX"
    if text in {"PACK", "팩"}:
        return "팩"
    if text in {"EA", "개"}:
        return "EA"
    if text == "KG":
        return "KG"
    return text


def _to_float(value: Any, default: float = 0.0) -> float:
    text = str(value or "").strip().replace(",", "")
    if text == "":
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _format_number(value: float) -> int | float:
    value = float(value)
    return int(round(value)) if value.is_integer() else round(value, 3)


def _month_text(date_text: str) -> str:
    try:
        month = int(str(date_text)[5:7])
    except Exception:
        return ""
    return f"{month}월"


def _statement_no(statement_data: dict[str, Any]) -> str:
    slip = statement_data.get("slip", {}) or {}
    return str(slip.get("trade_slip_de_no") or slip.get("trade_slip_no") or "").strip()


def _statement_date(statement_data: dict[str, Any]) -> str:
    return str((statement_data.get("slip", {}) or {}).get("date", "")).strip()


def _statement_total(statement_data: dict[str, Any]) -> float:
    return _to_float((statement_data.get("amount", {}) or {}).get("total", 0), 0.0)


def _open_purchase_spreadsheet(settings: Settings):
    if not settings.purchase_spreadsheet_id:
        raise PurchaseRecordError("PURCHASE_SPREADSHEET_ID 환경변수가 비어 있습니다. 매입단가 시트 ID를 설정해 주세요.")
    return open_spreadsheet_by_id(settings, settings.purchase_spreadsheet_id)


def _get_or_create_worksheet(spreadsheet, title: str, headers: list[str], rows: int = 1000, cols: int | None = None):
    try:
        ws = spreadsheet.worksheet(title)
    except WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols or max(len(headers), 8))

    current_headers = ws.row_values(1)
    if current_headers[: len(headers)] != headers:
        ws.update("A1", [headers], value_input_option="USER_ENTERED")
    return ws


def _safe_get_all_records(worksheet) -> list[dict[str, Any]]:
    try:
        return worksheet.get_all_records(default_blank="")
    except Exception:
        return []


def _hide_sheet(spreadsheet, worksheet) -> None:
    try:
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
    except Exception:
        # 숨김 실패가 본 처리 실패로 이어지지 않게 한다.
        pass


def _ensure_purchase_workbook(spreadsheet) -> dict[str, Any]:
    worksheets = {
        PURCHASE_RECORD_SHEET: _get_or_create_worksheet(spreadsheet, PURCHASE_RECORD_SHEET, PURCHASE_RECORD_HEADERS),
        CONVERSION_SHEET: _get_or_create_worksheet(spreadsheet, CONVERSION_SHEET, CONVERSION_HEADERS),
        UNIT_PRICE_SHEET: _get_or_create_worksheet(spreadsheet, UNIT_PRICE_SHEET, UNIT_PRICE_HEADERS, rows=1000, cols=10),
        UNREGISTERED_SHEET: _get_or_create_worksheet(spreadsheet, UNREGISTERED_SHEET, UNREGISTERED_HEADERS),
        RENAME_SHEET: _get_or_create_worksheet(spreadsheet, RENAME_SHEET, RENAME_HEADERS),
        SLIP_HISTORY_SHEET: _get_or_create_worksheet(spreadsheet, SLIP_HISTORY_SHEET, SLIP_HISTORY_HEADERS),
        SLIP_DETAIL_SHEET: _get_or_create_worksheet(spreadsheet, SLIP_DETAIL_SHEET, SLIP_DETAIL_HEADERS),
        DROPDOWN_SHEET: _get_or_create_worksheet(spreadsheet, DROPDOWN_SHEET, DROPDOWN_HEADERS, rows=1000, cols=2),
    }

    _hide_sheet(spreadsheet, worksheets[SLIP_DETAIL_SHEET])
    _hide_sheet(spreadsheet, worksheets[DROPDOWN_SHEET])
    _setup_unit_price_sheet(spreadsheet, worksheets[UNIT_PRICE_SHEET])
    _refresh_dropdown_lists(spreadsheet, worksheets)
    _setup_validations(spreadsheet, worksheets)
    return worksheets


def _setup_unit_price_sheet(spreadsheet, worksheet) -> None:
    values = worksheet.get_all_values()
    if len(values) < 1 or values[0][:6] != UNIT_PRICE_HEADERS:
        worksheet.update("A1", [UNIT_PRICE_HEADERS], value_input_option="USER_ENTERED")

    # 검색 조건 영역. 이미 값이 있으면 사용자가 선택한 조건을 덮어쓰지 않는다.
    if not worksheet.acell("H1").value:
        worksheet.update(
            "H1:I3",
            [["월 선택", "전체"], ["상품명 선택", "전체"], ["날짜 선택", ""]],
            value_input_option="USER_ENTERED",
        )

    # 결과표는 매입기록을 기준으로 월/상품명/날짜 조건을 동시에 적용한다.
    formula = (
        '=IFERROR(SORT(FILTER({매입기록!F2:F,매입기록!A2:A,매입기록!B2:B,매입기록!C2:C,매입기록!D2:D,매입기록!E2:E},'
        'IF($I$1="전체",LEN(매입기록!B2:B),매입기록!F2:F=$I$1),'
        'IF($I$2="전체",LEN(매입기록!B2:B),매입기록!B2:B=$I$2),'
        'IF($I$3="",LEN(매입기록!A2:A),매입기록!A2:A=$I$3)),2,TRUE,3,TRUE),"조건에 맞는 기록이 없습니다.")'
    )
    if not worksheet.acell("A2").value:
        worksheet.update("A2", [[formula]], value_input_option="USER_ENTERED")

    # 오른쪽에는 월별/상품별 평균단가 요약표를 둔다.
    if not worksheet.acell("K1").value:
        summary_formula = (
            '=IFERROR(QUERY(매입기록!A2:F,'
            '"select F,B,sum(C),sum(D),sum(D)/sum(C) '
            'where B is not null group by F,B '
            'label F \"월\", B \"상품명\", sum(C) \"총매입량\", '
            'sum(D) \"총매입금액\", sum(D)/sum(C) \"평균단가\"",0),"")'
        )
        worksheet.update("K1", [[summary_formula]], value_input_option="USER_ENTERED")


def _refresh_dropdown_lists(spreadsheet, worksheets: dict[str, Any]) -> None:
    dropdown_ws = worksheets[DROPDOWN_SHEET]
    conversion_ws = worksheets[CONVERSION_SHEET]
    purchase_ws = worksheets[PURCHASE_RECORD_SHEET]

    products: set[str] = set()
    for row in _safe_get_all_records(conversion_ws):
        name = str(row.get("변환품명", "")).strip()
        if name:
            products.add(name)
    for row in _safe_get_all_records(purchase_ws):
        name = str(row.get("상품명", "")).strip()
        if name:
            products.add(name)

    month_values = ["전체"] + [f"{month}월" for month in range(1, 13)]
    product_values = ["전체"] + sorted(products)
    max_len = max(len(month_values), len(product_values), 1)
    rows = []
    for idx in range(max_len):
        rows.append([
            month_values[idx] if idx < len(month_values) else "",
            product_values[idx] if idx < len(product_values) else "",
        ])
    dropdown_ws.clear()
    dropdown_ws.update("A1", [DROPDOWN_HEADERS] + rows, value_input_option="USER_ENTERED")


def _setup_validations(spreadsheet, worksheets: dict[str, Any]) -> None:
    unit_price_ws = worksheets[UNIT_PRICE_SHEET]
    dropdown_ws = worksheets[DROPDOWN_SHEET]
    try:
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": unit_price_ws.id,
                                "startRowIndex": 0,
                                "endRowIndex": 1,
                                "startColumnIndex": 8,
                                "endColumnIndex": 9,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_RANGE",
                                    "values": [{"userEnteredValue": f"='{DROPDOWN_SHEET}'!A2:A13"}],
                                },
                                "strict": False,
                                "showCustomUi": True,
                            },
                        }
                    },
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": unit_price_ws.id,
                                "startRowIndex": 1,
                                "endRowIndex": 2,
                                "startColumnIndex": 8,
                                "endColumnIndex": 9,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_RANGE",
                                    "values": [{"userEnteredValue": f"='{DROPDOWN_SHEET}'!B2:B1000"}],
                                },
                                "strict": False,
                                "showCustomUi": True,
                            },
                        }
                    },
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": unit_price_ws.id,
                                "startRowIndex": 2,
                                "endRowIndex": 3,
                                "startColumnIndex": 8,
                                "endColumnIndex": 9,
                            },
                            "rule": {
                                "condition": {"type": "DATE_IS_VALID"},
                                "strict": False,
                                "showCustomUi": True,
                            },
                        }
                    },
                ]
            }
        )
    except Exception:
        # 드롭다운 설정 실패가 본 처리 실패로 이어지지 않게 한다.
        pass


def _is_active(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if text in INACTIVE_VALUES:
        return False
    return text in ACTIVE_VALUES or bool(text)


def _load_conversion_rules(conversion_ws) -> dict[str, ConversionRule]:
    rows = _safe_get_all_records(conversion_ws)
    rules: dict[str, ConversionRule] = {}
    duplicated: list[str] = []

    for row in rows:
        original = str(row.get("원본품명", "")).strip()
        converted = str(row.get("변환품명", "")).strip()
        if not original:
            continue

        key = normalize_name(original)
        if key in rules:
            duplicated.append(original)
            continue

        unit = _normalize_unit(row.get("단위", ""))
        multiplier_text = str(row.get("1개당 환산수량", "")).strip()
        multiplier = _to_float(multiplier_text, 0.0)
        active = _is_active(row.get("사용여부", "사용"))
        rules[key] = ConversionRule(
            original_name=original,
            converted_name=converted,
            unit=unit,
            multiplier=multiplier,
            active=active,
        )

    if duplicated:
        duplicated_text = ", ".join(sorted(set(duplicated))[:20])
        raise PurchaseRecordError(f"품목환산표에 중복 원본품명이 있습니다: {duplicated_text}")

    return rules


def _aggregate_statement_rows(receipt_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in receipt_rows:
        name = str(row.get("name", "")).strip()
        unit = _normalize_unit(row.get("unit", ""))
        key = (name, unit)
        target = grouped.setdefault(
            key,
            {"name": name, "unit": unit, "quantity": 0.0, "sum_amount": 0.0},
        )
        target["quantity"] += _to_float(row.get("quantity", 0), 0.0)
        target["sum_amount"] += _to_float(row.get("sum_amount", 0), 0.0)
    return list(grouped.values())


def _resolve_row(row: dict[str, Any], rules: dict[str, ConversionRule]) -> tuple[str, str, float | None, str | None]:
    original = str(row.get("name", "")).strip()
    unit = _normalize_unit(row.get("unit", ""))
    quantity = _to_float(row.get("quantity", 0), 0.0)
    rule = rules.get(normalize_name(original))

    if rule is None:
        return "", "", None, "품목환산표 미등록"
    if not rule.active:
        return rule.converted_name, rule.unit or unit, None, "미사용제외"
    if not rule.converted_name:
        return "", rule.unit or unit, None, "변환품명 없음"

    expected_unit = rule.unit or unit
    if expected_unit and unit and expected_unit != unit:
        return rule.converted_name, expected_unit, None, "단위 불일치"

    multiplier = rule.multiplier
    if multiplier <= 0:
        return rule.converted_name, expected_unit, None, "환산수량 없음"

    return rule.converted_name, expected_unit, quantity * multiplier, None


def _append_purchase_record_rows(purchase_ws, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    start_row = len(purchase_ws.get_all_values()) + 1
    values = []
    for offset, row in enumerate(rows):
        sheet_row = start_row + offset
        values.append(
            [
                row["date"],
                row["product"],
                _format_number(row["quantity"]),
                _format_number(row["amount"]),
                f"=IFERROR(D{sheet_row}/C{sheet_row},\"\")",
                _month_text(row["date"]),
            ]
        )

    purchase_ws.append_rows(values, value_input_option="USER_ENTERED")
    return len(values)


def _append_unregistered_rows(unregistered_ws, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    values = [
        [
            row["upload_date"],
            row["slip_no"],
            row["date"],
            row["original"],
            row["unit"],
            _format_number(row["quantity"]),
            _format_number(row["amount"]),
            row["reason"],
            "처리대기",
        ]
        for row in rows
    ]
    unregistered_ws.append_rows(values, value_input_option="USER_ENTERED")
    return len(values)


def _detail_key(slip_no: str, original: str, unit: str) -> str:
    return f"{slip_no}||{normalize_name(original)}||{_normalize_unit(unit)}"


def _load_detail_map(detail_ws) -> dict[str, tuple[int, dict[str, Any]]]:
    rows = _safe_get_all_records(detail_ws)
    result: dict[str, tuple[int, dict[str, Any]]] = {}
    for idx, row in enumerate(rows, start=2):
        slip_no = str(row.get("전표번호", "")).strip()
        original = str(row.get("원본품명", "")).strip()
        unit = str(row.get("단위", "")).strip()
        if not slip_no or not original:
            continue
        result[_detail_key(slip_no, original, unit)] = (idx, row)
    return result


def _upsert_detail_rows(detail_ws, detail_rows: list[dict[str, Any]]) -> None:
    if not detail_rows:
        return
    existing = _load_detail_map(detail_ws)
    append_values: list[list[Any]] = []
    updates: list[tuple[int, list[Any]]] = []

    for row in detail_rows:
        values = [
            row["slip_no"],
            row["date"],
            row["original"],
            row.get("converted", ""),
            row["unit"],
            _format_number(row["quantity"]),
            _format_number(row["amount"]),
            row["status"],
        ]
        key = _detail_key(row["slip_no"], row["original"], row["unit"])
        if key in existing:
            row_index, _ = existing[key]
            updates.append((row_index, values))
        else:
            append_values.append(values)

    for row_index, values in updates:
        detail_ws.update(f"A{row_index}:H{row_index}", [values], value_input_option="USER_ENTERED")
    if append_values:
        detail_ws.append_rows(append_values, value_input_option="USER_ENTERED")


def _upsert_slip_history(history_ws, detail_ws, slip_no: str, date_text: str, total_amount: float | None = None) -> dict[str, Any]:
    detail_rows = [row for row in _safe_get_all_records(detail_ws) if str(row.get("전표번호", "")).strip() == slip_no]
    if not detail_rows:
        return {"slip_no": slip_no, "status": "처리실패"}

    detail_total = sum(_to_float(row.get("금액", 0), 0.0) for row in detail_rows)
    if total_amount is None or total_amount <= 0:
        total_amount = detail_total

    completed_amount = sum(
        _to_float(row.get("금액", 0), 0.0)
        for row in detail_rows
        if str(row.get("처리상태", "")).strip() == "입력완료"
    )
    unresolved_amount = sum(
        _to_float(row.get("금액", 0), 0.0)
        for row in detail_rows
        if str(row.get("처리상태", "")).strip() in UNRESOLVED_STATUSES
    )
    status = "처리완료" if unresolved_amount <= 0 else "부분처리"

    rows = _safe_get_all_records(history_ws)
    target_row = None
    for idx, row in enumerate(rows, start=2):
        if str(row.get("전표번호", "")).strip() == slip_no:
            target_row = idx
            break

    values = [
        slip_no,
        date_text,
        _format_number(total_amount),
        _format_number(completed_amount),
        _format_number(unresolved_amount),
        status,
    ]
    if target_row:
        history_ws.update(f"A{target_row}:F{target_row}", [values], value_input_option="USER_ENTERED")
    else:
        history_ws.append_rows([values], value_input_option="USER_ENTERED")

    return {
        "slip_no": slip_no,
        "status": status,
        "total_amount": _format_number(total_amount),
        "completed_amount": _format_number(completed_amount),
        "unresolved_amount": _format_number(unresolved_amount),
    }


def _slip_status(history_ws, slip_no: str) -> str | None:
    for row in _safe_get_all_records(history_ws):
        if str(row.get("전표번호", "")).strip() == slip_no:
            return str(row.get("처리상태", "")).strip() or None
    return None


def process_purchase_statement(settings: Settings, statement_data: dict[str, Any], receipt_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not settings.purchase_spreadsheet_id:
        return {
            "ok": False,
            "enabled": False,
            "message": "매입단가 시트 ID(PURCHASE_SPREADSHEET_ID)가 없어 매입기록 입력을 건너뛰었습니다.",
        }

    spreadsheet = _open_purchase_spreadsheet(settings)
    worksheets = _ensure_purchase_workbook(spreadsheet)
    rules = _load_conversion_rules(worksheets[CONVERSION_SHEET])

    slip_no = _statement_no(statement_data)
    date_text = _statement_date(statement_data)
    total_amount = _statement_total(statement_data)
    if not slip_no:
        raise PurchaseRecordError("거래명세서 전표번호를 찾지 못했습니다.")

    previous_status = _slip_status(worksheets[SLIP_HISTORY_SHEET], slip_no)
    if previous_status == "처리완료":
        return {
            "ok": True,
            "slip_no": slip_no,
            "status": "중복업로드",
            "message": "이미 처리완료된 거래명세서입니다. 중복 입력을 방지하기 위해 매입기록에 추가하지 않았습니다.",
        }

    detail_existing = _load_detail_map(worksheets[SLIP_DETAIL_SHEET])
    grouped_by_product: dict[str, dict[str, Any]] = {}
    unregistered_rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    upload_date = _now_date(settings)

    for row in _aggregate_statement_rows(receipt_rows):
        original = str(row["name"]).strip()
        unit = _normalize_unit(row["unit"])
        amount = _to_float(row["sum_amount"], 0.0)
        quantity = _to_float(row["quantity"], 0.0)
        key = _detail_key(slip_no, original, unit)
        existing = detail_existing.get(key)
        if existing and str(existing[1].get("처리상태", "")).strip() in COMPLETE_STATUSES:
            continue

        converted, expected_unit, converted_quantity, reason = _resolve_row(row, rules)
        status = "입력완료" if reason is None else reason

        if reason is None and converted_quantity is not None:
            product = grouped_by_product.setdefault(
                converted,
                {"date": date_text, "product": converted, "quantity": 0.0, "amount": 0.0},
            )
            product["quantity"] += converted_quantity
            product["amount"] += amount
        elif reason in UNRESOLVED_STATUSES or reason == "변환품명 없음":
            unregistered_rows.append(
                {
                    "upload_date": upload_date,
                    "slip_no": slip_no,
                    "date": date_text,
                    "original": original,
                    "unit": unit,
                    "quantity": quantity,
                    "amount": amount,
                    "reason": reason,
                }
            )

        detail_rows.append(
            {
                "slip_no": slip_no,
                "date": date_text,
                "original": original,
                "converted": converted,
                "unit": unit,
                "quantity": quantity,
                "amount": amount,
                "status": status,
            }
        )

    inserted_count = _append_purchase_record_rows(worksheets[PURCHASE_RECORD_SHEET], list(grouped_by_product.values()))
    unregistered_count = _append_unregistered_rows(worksheets[UNREGISTERED_SHEET], unregistered_rows)
    _upsert_detail_rows(worksheets[SLIP_DETAIL_SHEET], detail_rows)
    history = _upsert_slip_history(worksheets[SLIP_HISTORY_SHEET], worksheets[SLIP_DETAIL_SHEET], slip_no, date_text, total_amount)
    _refresh_dropdown_lists(spreadsheet, worksheets)

    return {
        "ok": True,
        "enabled": True,
        "slip_no": slip_no,
        "date": date_text,
        "inserted_rows": inserted_count,
        "unregistered_rows": unregistered_count,
        "history": history,
    }


def setup_purchase_workbook(settings: Settings) -> dict[str, Any]:
    spreadsheet = _open_purchase_spreadsheet(settings)
    worksheets = _ensure_purchase_workbook(spreadsheet)
    return {
        "ok": True,
        "message": "매입단가 시트 기본 구성을 확인/생성했습니다.",
        "sheets": list(worksheets.keys()),
    }


def reprocess_unregistered_items(settings: Settings) -> dict[str, Any]:
    spreadsheet = _open_purchase_spreadsheet(settings)
    worksheets = _ensure_purchase_workbook(spreadsheet)
    rules = _load_conversion_rules(worksheets[CONVERSION_SHEET])
    unregistered_ws = worksheets[UNREGISTERED_SHEET]
    detail_ws = worksheets[SLIP_DETAIL_SHEET]
    history_ws = worksheets[SLIP_HISTORY_SHEET]

    rows = _safe_get_all_records(unregistered_ws)
    grouped_by_product: dict[tuple[str, str, str], dict[str, Any]] = {}
    detail_updates: list[dict[str, Any]] = []
    processed_sheet_rows: list[int] = []
    still_waiting = 0
    affected_slips: dict[str, str] = {}

    for sheet_row, row in enumerate(rows, start=2):
        if str(row.get("처리상태", "")).strip() != "처리대기":
            continue

        slip_no = str(row.get("전표번호", "")).strip()
        date_text = str(row.get("매입일", "")).strip()
        original = str(row.get("원본품명", "")).strip()
        unit = _normalize_unit(row.get("단위", ""))
        quantity = _to_float(row.get("수량", 0), 0.0)
        amount = _to_float(row.get("금액", 0), 0.0)
        converted, expected_unit, converted_quantity, reason = _resolve_row(
            {"name": original, "unit": unit, "quantity": quantity, "sum_amount": amount},
            rules,
        )

        if reason is None and converted_quantity is not None:
            group_key = (slip_no, date_text, converted)
            group = grouped_by_product.setdefault(
                group_key,
                {"date": date_text, "product": converted, "quantity": 0.0, "amount": 0.0},
            )
            group["quantity"] += converted_quantity
            group["amount"] += amount
            detail_updates.append(
                {
                    "slip_no": slip_no,
                    "date": date_text,
                    "original": original,
                    "converted": converted,
                    "unit": unit,
                    "quantity": quantity,
                    "amount": amount,
                    "status": "입력완료",
                }
            )
            processed_sheet_rows.append(sheet_row)
            affected_slips[slip_no] = date_text
        else:
            still_waiting += 1
            # 새 사유로 갱신한다.
            try:
                unregistered_ws.update(f"H{sheet_row}:H{sheet_row}", [[reason or "처리불가"]], value_input_option="USER_ENTERED")
            except Exception:
                pass

    inserted_count = _append_purchase_record_rows(worksheets[PURCHASE_RECORD_SHEET], list(grouped_by_product.values()))
    _upsert_detail_rows(detail_ws, detail_updates)

    # 처리 완료된 미등록 행만 상태 변경한다.
    for sheet_row in processed_sheet_rows:
        unregistered_ws.update(f"I{sheet_row}:I{sheet_row}", [["처리완료"]], value_input_option="USER_ENTERED")

    histories = []
    for slip_no, date_text in affected_slips.items():
        histories.append(_upsert_slip_history(history_ws, detail_ws, slip_no, date_text, None))

    _refresh_dropdown_lists(spreadsheet, worksheets)

    return {
        "ok": True,
        "inserted_rows": inserted_count,
        "processed_unregistered_rows": len(processed_sheet_rows),
        "still_waiting_rows": still_waiting,
        "updated_slips": histories,
    }


def apply_product_renames(settings: Settings) -> dict[str, Any]:
    spreadsheet = _open_purchase_spreadsheet(settings)
    worksheets = _ensure_purchase_workbook(spreadsheet)
    rename_ws = worksheets[RENAME_SHEET]
    purchase_ws = worksheets[PURCHASE_RECORD_SHEET]

    rename_rows = _safe_get_all_records(rename_ws)
    purchase_values = purchase_ws.get_all_values()
    if len(purchase_values) <= 1:
        return {"ok": True, "renamed_cells": 0, "message": "매입기록에 변경할 데이터가 없습니다."}

    product_col = PURCHASE_RECORD_HEADERS.index("상품명") + 1
    renamed_cells = 0
    successful_rename_rows: list[int] = []
    failed_rows: list[tuple[int, str]] = []

    for sheet_row, row in enumerate(rename_rows, start=2):
        old_name = str(row.get("기존상품명", "")).strip()
        new_name = str(row.get("변경상품명", "")).strip()
        if not old_name and not new_name:
            successful_rename_rows.append(sheet_row)
            continue
        if not old_name or not new_name:
            failed_rows.append((sheet_row, "기존상품명과 변경상품명을 모두 입력해 주세요."))
            continue
        if old_name == new_name:
            successful_rename_rows.append(sheet_row)
            continue

        matched_rows = []
        for purchase_row_index, purchase_row in enumerate(purchase_values[1:], start=2):
            current_name = purchase_row[product_col - 1].strip() if len(purchase_row) >= product_col else ""
            if current_name == old_name:
                matched_rows.append(purchase_row_index)

        if not matched_rows:
            failed_rows.append((sheet_row, "매입기록에서 기존상품명을 찾을 수 없음"))
            continue

        for purchase_row_index in matched_rows:
            purchase_ws.update_cell(purchase_row_index, product_col, new_name)
            renamed_cells += 1
        successful_rename_rows.append(sheet_row)

    # 성공한 상품명변경 행은 아래에서부터 삭제한다.
    for sheet_row in sorted(set(successful_rename_rows), reverse=True):
        try:
            rename_ws.delete_rows(sheet_row)
        except Exception:
            pass

    # 실패 행 오류내용 갱신
    for sheet_row, message in failed_rows:
        try:
            rename_ws.update(f"C{sheet_row}:C{sheet_row}", [[message]], value_input_option="USER_ENTERED")
        except Exception:
            pass

    _refresh_dropdown_lists(spreadsheet, worksheets)

    return {
        "ok": True,
        "renamed_cells": renamed_cells,
        "success_rows": len(successful_rename_rows),
        "failed_rows": len(failed_rows),
    }
