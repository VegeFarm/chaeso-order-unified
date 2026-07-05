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
DROPDOWN_SHEET = "설정"

PURCHASE_RECORD_HEADERS = ["매입일", "상품명", "매입량", "매입총액", "단가", "월"]
CONVERSION_HEADERS = ["원본품명", "변환품명", "단위", "1개당 환산수량", "사용여부"]
UNIT_PRICE_HEADERS = ["월", "매입일", "상품명", "매입량", "매입총액", "단가"]
UNREGISTERED_HEADERS = ["업로드일", "전표번호", "매입일", "원본품명", "단위", "수량", "금액", "사유", "처리상태"]
RENAME_HEADERS = ["기존상품명", "변경상품명", "오류내용"]
SLIP_HISTORY_HEADERS = ["전표번호", "매입일", "총금액", "입력완료금액", "미입력금액", "처리상태"]
SLIP_DETAIL_HEADERS = ["전표번호", "매입일", "원본품명", "변환품명", "단위", "수량", "금액", "처리상태"]
DROPDOWN_HEADERS = ["상품명목록", "월목록"]

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


def _get_template_worksheet(spreadsheet, title: str, headers: list[str], rows: int = 1000, cols: int | None = None):
    """
    사용자가 엑셀 템플릿을 구글시트로 가져와 둔 상태를 전제로 한다.
    시트를 새로 만들거나 디자인을 덮어쓰지 않고, 필요한 시트가 없으면 명확한 오류를 낸다.
    단, 프로그램 동작에 꼭 필요한 헤더/행열 수만 보완한다.
    """
    required_cols = cols or max(len(headers), 8)
    try:
        ws = spreadsheet.worksheet(title)
    except WorksheetNotFound as exc:
        raise PurchaseRecordError(
            f"매입단가 템플릿에 '{title}' 시트가 없습니다. "
            "제공한 엑셀 템플릿을 구글시트로 가져온 뒤 PURCHASE_SPREADSHEET_ID를 그 시트 ID로 설정해 주세요."
        ) from exc

    # 템플릿 디자인은 그대로 두고, 데이터가 늘어날 공간만 안전하게 확장한다.
    try:
        if ws.row_count < rows or ws.col_count < required_cols:
            ws.resize(rows=max(ws.row_count, rows), cols=max(ws.col_count, required_cols))
    except Exception:
        pass

    # 템플릿에 헤더가 없거나 관리용 컬럼이 부족한 경우 값만 보완한다.
    # 서식/디자인은 건드리지 않는다.
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


def _color(hex_color: str) -> dict[str, float]:
    text = hex_color.strip().lstrip("#")
    return {
        "red": int(text[0:2], 16) / 255,
        "green": int(text[2:4], 16) / 255,
        "blue": int(text[4:6], 16) / 255,
    }


def _grid(worksheet, start_row: int, end_row: int, start_col: int, end_col: int) -> dict[str, int]:
    return {
        "sheetId": worksheet.id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


def _repeat_cell(worksheet, start_row: int, end_row: int, start_col: int, end_col: int, user_format: dict[str, Any], fields: str) -> dict[str, Any]:
    return {
        "repeatCell": {
            "range": _grid(worksheet, start_row, end_row, start_col, end_col),
            "cell": {"userEnteredFormat": user_format},
            "fields": f"userEnteredFormat({fields})",
        }
    }


def _set_col_width(worksheet, start_col: int, end_col: int, pixel_size: int) -> dict[str, Any]:
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "COLUMNS",
                "startIndex": start_col,
                "endIndex": end_col,
            },
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize",
        }
    }


def _set_row_height(worksheet, start_row: int, end_row: int, pixel_size: int) -> dict[str, Any]:
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "ROWS",
                "startIndex": start_row,
                "endIndex": end_row,
            },
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize",
        }
    }


def _merge_range(worksheet, start_row: int, end_row: int, start_col: int, end_col: int) -> list[dict[str, Any]]:
    grid = _grid(worksheet, start_row, end_row, start_col, end_col)
    return [
        {"unmergeCells": {"range": grid}},
        {"mergeCells": {"range": grid, "mergeType": "MERGE_ALL"}},
    ]


def _apply_purchase_workbook_design(spreadsheet, worksheets: dict[str, Any]) -> None:
    """엑셀 템플릿과 최대한 같은 색상/너비/서식을 구글시트에 적용한다."""
    teal = _color("#0F766E")
    light_green = _color("#D9EAD3")
    white = _color("#FFFFFF")

    header_fmt = {
        "backgroundColor": teal,
        "textFormat": {"foregroundColor": white, "bold": True, "fontSize": 10, "fontFamily": "Arial"},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    }
    section_fmt = header_fmt
    label_fmt = {
        "backgroundColor": light_green,
        "textFormat": {"bold": True, "fontSize": 10, "fontFamily": "Arial"},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    }
    body_fmt = {
        "textFormat": {"fontSize": 10, "fontFamily": "Arial"},
        "verticalAlignment": "MIDDLE",
    }
    center_fmt = {**body_fmt, "horizontalAlignment": "CENTER"}
    # 주의: "#,##0.###"처럼 소수점 뒤가 전부 #인 패턴은 정수(예: 65)도 "65."로 표시된다.
    # numberFormat을 넣지 않고 필드마스크에만 numberFormat을 남기면
    # 기존에 걸려 있던 잘못된 서식이 지워지고 '자동' 표시가 된다. (1 -> 1, 0.5 -> 0.5)
    number_fmt = {**center_fmt}
    money_fmt = {**center_fmt, "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}
    date_fmt = {**center_fmt, "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}}
    text_fmt = {**center_fmt, "numberFormat": {"type": "TEXT", "pattern": "@"}}

    widths = {
        PURCHASE_RECORD_SHEET: [105, 145, 95, 115, 95, 80],
        CONVERSION_SHEET: [175, 145, 80, 130, 95],
        UNIT_PRICE_SHEET: [80, 110, 145, 95, 115, 95, 28],
        UNREGISTERED_SHEET: [105, 145, 105, 175, 80, 80, 100, 175, 95],
        RENAME_SHEET: [160, 160, 190],
        SLIP_HISTORY_SHEET: [145, 105, 115, 115, 115, 95],
        SLIP_DETAIL_SHEET: [145, 105, 175, 145, 95, 95, 115, 105],
        DROPDOWN_SHEET: [160, 90],
    }

    requests: list[dict[str, Any]] = []
    for title, ws in worksheets.items():
        col_widths = widths.get(title, [])
        for col_index, width in enumerate(col_widths):
            requests.append(_set_col_width(ws, col_index, col_index + 1, width))
        requests.append(_set_row_height(ws, 0, 1, 24))
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount",
                }
            }
        )
        # 기본 글꼴
        requests.append(_repeat_cell(ws, 0, 1000, 0, max(len(col_widths), 1), body_fmt, "textFormat,verticalAlignment"))

    # 일반 표 헤더
    header_ranges = {
        PURCHASE_RECORD_SHEET: 6,
        CONVERSION_SHEET: 5,
        UNREGISTERED_SHEET: 9,
        RENAME_SHEET: 3,
        SLIP_HISTORY_SHEET: 6,
        SLIP_DETAIL_SHEET: 8,
        DROPDOWN_SHEET: 2,
    }
    for title, cols in header_ranges.items():
        requests.append(_repeat_cell(worksheets[title], 0, 1, 0, cols, header_fmt, "backgroundColor,textFormat,horizontalAlignment,verticalAlignment"))

    # 원물단가표 검색 화면 레이아웃
    unit_ws = worksheets[UNIT_PRICE_SHEET]
    requests.extend(_merge_range(unit_ws, 0, 1, 0, 6))   # A1:F1
    requests.append(_repeat_cell(unit_ws, 0, 1, 0, 6, section_fmt, "backgroundColor,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(unit_ws, 1, 4, 0, 1, label_fmt, "backgroundColor,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(unit_ws, 5, 6, 0, 6, header_fmt, "backgroundColor,textFormat,horizontalAlignment,verticalAlignment"))

    # 날짜/숫자/금액 서식
    requests.append(_repeat_cell(worksheets[PURCHASE_RECORD_SHEET], 1, 1000, 0, 1, date_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[PURCHASE_RECORD_SHEET], 1, 1000, 2, 3, number_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[PURCHASE_RECORD_SHEET], 1, 1000, 3, 5, money_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[PURCHASE_RECORD_SHEET], 1, 1000, 5, 6, text_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))

    requests.append(_repeat_cell(worksheets[CONVERSION_SHEET], 1, 1000, 3, 4, number_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[UNREGISTERED_SHEET], 1, 1000, 0, 1, date_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[UNREGISTERED_SHEET], 1, 1000, 2, 3, date_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[UNREGISTERED_SHEET], 1, 1000, 5, 6, number_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[UNREGISTERED_SHEET], 1, 1000, 6, 7, money_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))

    requests.append(_repeat_cell(worksheets[SLIP_HISTORY_SHEET], 1, 1000, 1, 2, date_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[SLIP_HISTORY_SHEET], 1, 1000, 2, 5, money_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[SLIP_DETAIL_SHEET], 1, 5000, 1, 2, date_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[SLIP_DETAIL_SHEET], 1, 5000, 5, 6, number_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(worksheets[SLIP_DETAIL_SHEET], 1, 5000, 6, 7, money_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))

    requests.append(_repeat_cell(unit_ws, 3, 4, 1, 2, date_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(unit_ws, 6, 1000, 1, 2, date_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(unit_ws, 6, 1000, 3, 4, number_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))
    requests.append(_repeat_cell(unit_ws, 6, 1000, 4, 6, money_fmt, "numberFormat,textFormat,horizontalAlignment,verticalAlignment"))

    try:
        # Google Sheets API는 한 번에 너무 많은 요청을 보내면 실패할 수 있으므로 나눠서 보낸다.
        for start in range(0, len(requests), 80):
            spreadsheet.batch_update({"requests": requests[start : start + 80]})
    except Exception:
        # 디자인 적용 실패 때문에 매입기록 입력 자체가 막히지 않게 한다.
        pass


def _load_purchase_template_workbook(spreadsheet, *, maintenance: bool = False) -> dict[str, Any]:
    worksheets = {
        PURCHASE_RECORD_SHEET: _get_template_worksheet(spreadsheet, PURCHASE_RECORD_SHEET, PURCHASE_RECORD_HEADERS, rows=1000, cols=6),
        CONVERSION_SHEET: _get_template_worksheet(spreadsheet, CONVERSION_SHEET, CONVERSION_HEADERS, rows=1000, cols=5),
        UNIT_PRICE_SHEET: _get_template_worksheet(spreadsheet, UNIT_PRICE_SHEET, UNIT_PRICE_HEADERS, rows=1000, cols=16),
        UNREGISTERED_SHEET: _get_template_worksheet(spreadsheet, UNREGISTERED_SHEET, UNREGISTERED_HEADERS, rows=1000, cols=9),
        RENAME_SHEET: _get_template_worksheet(spreadsheet, RENAME_SHEET, RENAME_HEADERS, rows=300, cols=3),
        SLIP_HISTORY_SHEET: _get_template_worksheet(spreadsheet, SLIP_HISTORY_SHEET, SLIP_HISTORY_HEADERS, rows=1000, cols=6),
        SLIP_DETAIL_SHEET: _get_template_worksheet(spreadsheet, SLIP_DETAIL_SHEET, SLIP_DETAIL_HEADERS, rows=5000, cols=8),
        DROPDOWN_SHEET: _get_template_worksheet(spreadsheet, DROPDOWN_SHEET, DROPDOWN_HEADERS, rows=1000, cols=2),
    }

    # 품목환산표 D열(1개당 환산수량)은 실행 후에도 1.처럼 보이지 않게 최소 보정한다.
    _normalize_conversion_multiplier_display(spreadsheet, worksheets[CONVERSION_SHEET])

    # 품목환산표 E열(사용여부)은 Y/N 드롭다운 칩처럼 보이도록 보정한다.
    _apply_conversion_usage_dropdown(spreadsheet, worksheets[CONVERSION_SHEET])

    # 거래명세서 실행 때마다 서식/드롭다운/숨김을 반복 적용하면
    # Google Sheets API 1분 쓰기 제한(429)에 걸릴 수 있다.
    # 기본 실행은 값 입력에 필요한 최소 확인만 하고,
    # 템플릿 점검이 필요할 때만 maintenance=True로 보완 작업을 수행한다.
    if maintenance:
        _hide_sheet(spreadsheet, worksheets[SLIP_DETAIL_SHEET])
        _hide_sheet(spreadsheet, worksheets[DROPDOWN_SHEET])
        _apply_template_quick_fixes(spreadsheet, worksheets)
        _refresh_dropdown_lists(spreadsheet, worksheets)
        _setup_validations(spreadsheet, worksheets)

    # 원물단가표 조회 수식은 값 1회 업데이트만 사용하고,
    # 이미 들어가 있으면 쓰기 요청을 보내지 않는다.
    _sync_unit_price_template_values(worksheets[UNIT_PRICE_SHEET])

    # 원물단가표 평균단가 요약(I2:I4)이 "5,000." 처럼 보이지 않게 서식을 보정한다.
    _fix_unit_price_summary_format(spreadsheet, worksheets[UNIT_PRICE_SHEET])

    # 원물단가표 매입일 표시가 46206 같은 날짜 일련번호로 보이지 않게 하고,
    # 날짜선택(B4)에 구글시트 날짜 선택 달력이 뜨도록 최소 보정한다.
    _fix_unit_price_date_format_and_calendar(spreadsheet, worksheets[UNIT_PRICE_SHEET])
    return worksheets


def _fix_unit_price_date_format_and_calendar(spreadsheet, unit_ws) -> None:
    """
    원물단가표 날짜 표시/선택 보정.
    - 조회 결과의 매입일(B7:B1000)이 46206 같은 숫자로 보이면 날짜 서식을 다시 적용한다.
    - 날짜선택 셀(B4)에 DATE_IS_VALID 유효성 검사를 걸어 구글시트 날짜 선택 달력을 사용할 수 있게 한다.

    참고: B4는 "선택안함" 텍스트도 허용해야 하므로 strict=False로 둔다.
    """
    date_format = {
        "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    }
    try:
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": unit_ws.id,
                                "startRowIndex": 3,   # B4
                                "endRowIndex": 4,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
                            },
                            "cell": {"userEnteredFormat": date_format},
                            "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
                        }
                    },
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": unit_ws.id,
                                "startRowIndex": 6,   # B7:B1000 조회 결과 매입일
                                "endRowIndex": 1000,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
                            },
                            "cell": {"userEnteredFormat": date_format},
                            "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
                        }
                    },
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": unit_ws.id,
                                "startRowIndex": 3,
                                "endRowIndex": 4,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
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
        # 표시/달력 보정 실패가 매입기록 입력 실패로 이어지지 않게 한다.
        pass


def _fix_unit_price_summary_format(spreadsheet, unit_ws) -> None:
    """
    원물단가표 I2:I4(평균단가/총 매입량/총 매입금액) 표시 서식을 보정한다.
    "#,##0.###"처럼 소수점 뒤가 전부 #인 패턴은 정수도 "5,000."으로 표시되므로,
    소수점이 없는 "#,##0" 패턴으로 강제한다. (표시만 반올림되고 실제 값은 유지됨)
    """
    try:
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": unit_ws.id,
                                "startRowIndex": 1,   # 2행
                                "endRowIndex": 4,     # 4행까지
                                "startColumnIndex": 8,  # I열
                                "endColumnIndex": 9,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                                }
                            },
                            "fields": "userEnteredFormat.numberFormat",
                        }
                    }
                ]
            }
        )
    except Exception:
        # 표시 보정 실패가 매입기록 입력 실패로 이어지지 않게 한다.
        pass


def _check_purchase_template(spreadsheet) -> dict[str, Any]:
    worksheets = _load_purchase_template_workbook(spreadsheet, maintenance=True)
    return {
        "ok": True,
        "message": "매입단가 템플릿 연결을 확인했습니다. 시트 디자인은 새로 만들거나 초기화하지 않았습니다.",
        "sheets": list(worksheets.keys()),
    }

def _apply_template_quick_fixes(spreadsheet, worksheets: dict[str, Any]) -> None:
    """템플릿 디자인은 유지하면서 사용 중 불편한 표시/선택값만 보완한다."""
    try:
        conversion_ws = worksheets[CONVERSION_SHEET]
        # 엑셀/구글시트에서 1개당 환산수량이 1. 처럼 보이지 않도록
        # numberFormat을 아예 지워 '자동' 표시로 만든다.
        # ("#,##0.########" 같은 패턴은 정수도 "1."로 표시되는 원인이었음)
        number_fmt = {
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
        }
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": conversion_ws.id,
                                "startRowIndex": 1,
                                "endRowIndex": 1000,
                                "startColumnIndex": 3,
                                "endColumnIndex": 4,
                            },
                            "cell": {"userEnteredFormat": number_fmt},
                            "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
                        }
                    }
                ]
            }
        )
    except Exception:
        pass


def _normalize_conversion_multiplier_display(spreadsheet, conversion_ws) -> dict[str, Any]:
    """
    품목환산표 D열 표시가 1. 처럼 남는 경우를 방지한다.
    - 표시값이 1. / 1.0 / 1.000이면 실제 숫자 1로 다시 저장한다.
    - D열 숫자 서식을 Google Sheets API 필드마스크로 강제 적용한다.
    """
    changed = 0
    try:
        values = conversion_ws.get("D2:D1000")
    except Exception:
        values = []

    updates: list[dict[str, Any]] = []
    for idx, row in enumerate(values, start=2):
        raw = str(row[0]).strip() if row else ""
        if not raw:
            continue
        normalized = raw.replace(",", "")
        try:
            number = float(normalized)
        except ValueError:
            continue

        clean_value: int | float
        if number.is_integer():
            clean_value = int(number)
            clean_text = str(clean_value)
        else:
            clean_value = round(number, 8)
            clean_text = (f"{clean_value:.8f}").rstrip("0").rstrip(".")

        # 표시값이 숫자 표준형과 다르면 셀 값을 다시 저장한다.
        # 예: 1. / 1.0 / 1.000 -> 1, 0.500 -> 0.5
        if raw != clean_text:
            updates.append({"range": f"D{idx}", "values": [[clean_value]]})
            changed += 1

    if updates:
        try:
            # RAW로 넣어 문자열 '1.'이 다시 남지 않게 한다.
            conversion_ws.batch_update(updates, value_input_option="RAW")
        except Exception:
            changed = 0

    # 기존 템플릿/구글시트에 남아 있는 0. 또는 1. 표시 서식을 강제로 덮어쓴다.
    try:
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": conversion_ws.id,
                                "startRowIndex": 1,
                                "endRowIndex": 1000,
                                "startColumnIndex": 3,
                                "endColumnIndex": 4,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    # numberFormat을 비워서 '자동' 표시로 초기화한다.
                                    # "0.########" 패턴은 정수 1을 "1."로 표시하는 원인이었음.
                                    "horizontalAlignment": "CENTER",
                                    "verticalAlignment": "MIDDLE",
                                }
                            },
                            "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
                        }
                    }
                ]
            }
        )
    except Exception:
        pass

    return {"normalized_multiplier_cells": changed}

def _normalize_usage_value(value: Any) -> str:
    """품목환산표 사용여부 표시값을 구글시트 드롭다운용 Y/N으로 정리한다."""
    text = str(value or "").strip()
    lower = text.lower()
    if lower in INACTIVE_VALUES:
        return "N"
    if lower in ACTIVE_VALUES or text in {"Y", "N"}:
        return "Y" if lower != "n" else "N"
    # 알 수 없는 값은 사용자가 직접 넣은 값일 수 있으므로 그대로 둔다.
    return text

def _normalize_conversion_usage_display(conversion_ws) -> dict[str, Any]:
    """
    품목환산표 E열 사용여부를 Y/N 표시로 정리한다.
    원본품명이 있는 행만 대상으로 하며, 빈 사용여부는 Y로 보정한다.
    """
    changed = 0
    try:
        values = conversion_ws.get("A2:E1000")
    except Exception:
        values = []

    updates: list[dict[str, Any]] = []
    for idx, row in enumerate(values, start=2):
        original = str(row[0]).strip() if len(row) > 0 else ""
        if not original:
            continue
        current = str(row[4]).strip() if len(row) > 4 else ""
        normalized = _normalize_usage_value(current)
        if normalized in {"Y", "N"} and current != normalized:
            updates.append({"range": f"E{idx}", "values": [[normalized]]})
            changed += 1

    if updates:
        try:
            conversion_ws.batch_update(updates, value_input_option="USER_ENTERED")
        except Exception:
            changed = 0
    return {"normalized_usage_cells": changed}

def _apply_conversion_usage_dropdown(spreadsheet, conversion_ws) -> None:
    """
    품목환산표 사용여부(E열)를 구글시트 드롭다운 칩처럼 보이도록 설정한다.
    Sheets API에서 드롭다운 색상 자체를 직접 지정하는 기능은 제한적이므로,
    Y/N 드롭다운 + 조건부 서식으로 이미지와 최대한 비슷하게 맞춘다.
    """
    _normalize_conversion_usage_display(conversion_ws)

    sheet_id = conversion_ws.id
    delete_requests: list[dict[str, Any]] = []
    try:
        metadata = spreadsheet.fetch_sheet_metadata()
        for sheet in metadata.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("sheetId") != sheet_id:
                continue
            rules = sheet.get("conditionalFormats", []) or []
            # E열(0-index 4)만 대상으로 걸린 기존 조건부 서식은 중복을 막기 위해 제거한다.
            for idx in reversed(range(len(rules))):
                rule = rules[idx] or {}
                ranges = rule.get("ranges", []) or []
                if any(
                    r.get("sheetId") == sheet_id
                    and r.get("startColumnIndex", 0) <= 4
                    and r.get("endColumnIndex", 0) >= 5
                    for r in ranges
                ):
                    delete_requests.append(
                        {
                            "deleteConditionalFormatRule": {
                                "sheetId": sheet_id,
                                "index": idx,
                            }
                        }
                    )
            break
    except Exception:
        delete_requests = []

    usage_range = {
        "sheetId": sheet_id,
        "startRowIndex": 1,
        "endRowIndex": 1000,
        "startColumnIndex": 4,
        "endColumnIndex": 5,
    }
    requests: list[dict[str, Any]] = []
    requests.extend(delete_requests)
    requests.extend(
        [
            {
                "setDataValidation": {
                    "range": usage_range,
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [
                                {"userEnteredValue": "Y"},
                                {"userEnteredValue": "N"},
                            ],
                        },
                        "strict": True,
                        "showCustomUi": True,
                    },
                }
            },
            _repeat_cell(
                conversion_ws,
                1,
                1000,
                4,
                5,
                {
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "textFormat": {"fontFamily": "Arial", "fontSize": 10},
                },
                "horizontalAlignment,verticalAlignment,textFormat",
            ),
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [usage_range],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": "N"}],
                            },
                            "format": {
                                "backgroundColor": _color("#F4CCCC"),
                                "textFormat": {"foregroundColor": _color("#CC0000"), "bold": True},
                            },
                        },
                    },
                    "index": 0,
                }
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [usage_range],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": "Y"}],
                            },
                            "format": {
                                "backgroundColor": _color("#EDEDED"),
                                "textFormat": {"foregroundColor": _color("#000000")},
                            },
                        },
                    },
                    "index": 0,
                }
            },
        ]
    )
    try:
        spreadsheet.batch_update({"requests": requests})
    except Exception:
        # 표시 보정 실패가 매입기록 입력 실패로 이어지지 않게 한다.
        pass

def _sync_unit_price_template_values(worksheet) -> None:
    """
    원물단가표 조회 수식/평균단가 요약 값만 보완한다.
    이미 같은 값이 들어 있으면 쓰기 요청을 보내지 않아 429 제한을 줄인다.
    서식/배경/병합은 템플릿 파일 기준으로 유지한다.
    """
    try:
        current = worksheet.get("A1:I7")
    except Exception:
        current = []

    def cell(row: int, col: int) -> str:
        try:
            return str(current[row - 1][col - 1] or "").strip()
        except Exception:
            return ""

    updates: list[dict[str, Any]] = []

    base_values = {
        "A1": "매입단가 조회",
        "A2": "월 선택",
        "A3": "상품명 선택",
        "A4": "날짜 선택",
        "B2": "전체",
        "B3": "전체",
        "B4": "선택안함",
        "A6": "월",
        "B6": "매입일",
        "C6": "상품명",
        "D6": "매입량",
        "E6": "매입총액",
        "F6": "단가",
    }
    for a1, value in base_values.items():
        col_letter = ''.join(ch for ch in a1 if ch.isalpha())
        row_number = int(''.join(ch for ch in a1 if ch.isdigit()))
        col_number = ord(col_letter) - ord('A') + 1
        if not cell(row_number, col_number):
            updates.append({"range": a1, "values": [[value]]})

    result_formula = (
        '=IFERROR(SORT(FILTER({매입기록!F2:F,매입기록!A2:A,매입기록!B2:B,매입기록!C2:C,매입기록!D2:D,매입기록!E2:E},'
        'IF(OR($B$4="",$B$4="선택안함"),IF($B$2="전체",LEN(매입기록!B2:B),매입기록!F2:F=$B$2),매입기록!A2:A=$B$4),'
        'IF($B$3="전체",LEN(매입기록!B2:B),매입기록!B2:B=$B$3)),2,TRUE,3,TRUE),"조건에 맞는 기록이 없습니다.")'
    )
    condition_expr = (
        'IF(OR($B$4="",$B$4="선택안함"),'
        'IF($B$2="전체",LEN(매입기록!B2:B),매입기록!F2:F=$B$2),'
        '매입기록!A2:A=$B$4)'
    )
    product_expr = 'IF($B$3="전체",LEN(매입기록!B2:B),매입기록!B2:B=$B$3)'
    total_qty_formula = f'=IFERROR(SUM(FILTER(매입기록!C2:C,{condition_expr},{product_expr})),0)'
    total_amount_formula = f'=IFERROR(SUM(FILTER(매입기록!D2:D,{condition_expr},{product_expr})),0)'
    avg_price_formula = '=IFERROR(I4/I3,"")'

    desired_cells = {
        "A7": result_formula,
        "H1": "평균단가 요약",
        "H2": "평균단가",
        "H3": "총 매입량",
        "H4": "총 매입금액",
        "I2": avg_price_formula,
        "I3": total_qty_formula,
        "I4": total_amount_formula,
    }
    for a1, value in desired_cells.items():
        col_letter = ''.join(ch for ch in a1 if ch.isalpha())
        row_number = int(''.join(ch for ch in a1 if ch.isdigit()))
        col_number = ord(col_letter) - ord('A') + 1
        current_value = cell(row_number, col_number)
        # 구글시트 수식은 get 결과가 표시값으로 올 수 있어, 수식 칸은 비어 있을 때만 보완한다.
        if current_value == "":
            updates.append({"range": a1, "values": [[value]]})

    if updates:
        try:
            worksheet.batch_update(updates, value_input_option="USER_ENTERED")
        except Exception:
            pass

def _refresh_dropdown_lists(spreadsheet, worksheets: dict[str, Any]) -> None:
    """설정 시트의 드롭다운 목록을 갱신하되, 내용이 같으면 쓰기 요청을 보내지 않는다."""
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

    product_values = ["전체"] + sorted(products)
    month_values = ["전체"] + [f"{month}월" for month in range(1, 13)]
    max_len = max(len(month_values), len(product_values), 1)
    rows = [[DROPDOWN_HEADERS[0], DROPDOWN_HEADERS[1]]]
    for idx in range(max_len):
        rows.append([
            product_values[idx] if idx < len(product_values) else "",
            month_values[idx] if idx < len(month_values) else "",
        ])

    # 예전 데이터가 더 길게 남아 있던 경우까지 지울 수 있게 충분한 빈 줄을 포함한다.
    target_len = max(1000, len(rows))
    padded_rows = rows + [["", ""] for _ in range(target_len - len(rows))]

    try:
        current = dropdown_ws.get(f"A1:B{target_len}")
    except Exception:
        current = []

    def normalize_grid(values: list[list[Any]], length: int) -> list[list[str]]:
        normalized: list[list[str]] = []
        for idx in range(length):
            row = values[idx] if idx < len(values) else []
            normalized.append([
                str(row[0]).strip() if len(row) > 0 else "",
                str(row[1]).strip() if len(row) > 1 else "",
            ])
        return normalized

    if normalize_grid(current, target_len) == normalize_grid(padded_rows, target_len):
        return

    try:
        dropdown_ws.update(f"A1:B{target_len}", padded_rows, value_input_option="USER_ENTERED")
    except Exception:
        pass

def _setup_validations(spreadsheet, worksheets: dict[str, Any]) -> None:
    unit_price_ws = worksheets[UNIT_PRICE_SHEET]
    try:
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": unit_price_ws.id,
                                "startRowIndex": 1,
                                "endRowIndex": 2,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_RANGE",
                                    "values": [{"userEnteredValue": f"='{DROPDOWN_SHEET}'!B2:B13"}],
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
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_RANGE",
                                    "values": [{"userEnteredValue": f"='{DROPDOWN_SHEET}'!A2:A1000"}],
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
                                "startRowIndex": 3,
                                "endRowIndex": 4,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
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
    worksheets = _load_purchase_template_workbook(spreadsheet)
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

    # 전표관리만 삭제하고 같은 거래명세서를 다시 넣는 경우,
    # 전표상세관리의 과거 입력완료 행 때문에 매입기록 추가가 0행이 되는 문제가 있었다.
    # 부분처리 전표를 이어서 처리할 때만 기존 전표상세관리 완료 행을 건너뛰고,
    # 전표관리 기록이 없으면 재업로드 의도로 보고 전표상세관리 기존 행을 무시한다.
    all_detail_existing = _load_detail_map(worksheets[SLIP_DETAIL_SHEET])
    detail_existing = all_detail_existing if previous_status == "부분처리" else {}
    reupload_after_history_deleted = previous_status is None and any(
        key.startswith(f"{slip_no}||") for key in all_detail_existing.keys()
    )
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
        "reupload_after_history_deleted": reupload_after_history_deleted,
    }


def delete_purchase_records(settings: Settings, date_text: str, product_name: str = "") -> dict[str, Any]:
    """매입기록 시트에서 매입일 또는 매입일+상품명 조건으로 행을 삭제한다."""
    clean_date = str(date_text or "").strip()
    clean_product = str(product_name or "").strip()
    if not clean_date:
        raise PurchaseRecordError("삭제할 매입일을 선택해 주세요.")
    if clean_product in {"전체", "선택안함"}:
        clean_product = ""

    spreadsheet = _open_purchase_spreadsheet(settings)
    worksheets = _load_purchase_template_workbook(spreadsheet)
    purchase_ws = worksheets[PURCHASE_RECORD_SHEET]
    values = purchase_ws.get_all_values()
    if len(values) <= 1:
        return {
            "ok": True,
            "deleted_rows": 0,
            "date": clean_date,
            "product": clean_product or "전체",
            "message": "매입기록에 삭제할 데이터가 없습니다.",
        }

    matched_rows: list[int] = []
    for idx, row in enumerate(values[1:], start=2):
        row_date = str(row[0]).strip() if len(row) > 0 else ""
        row_product = str(row[1]).strip() if len(row) > 1 else ""
        if row_date == clean_date and (not clean_product or row_product == clean_product):
            matched_rows.append(idx)

    for row_index in sorted(matched_rows, reverse=True):
        purchase_ws.delete_rows(row_index)

    if matched_rows:
        _refresh_dropdown_lists(spreadsheet, worksheets)

    return {
        "ok": True,
        "deleted_rows": len(matched_rows),
        "date": clean_date,
        "product": clean_product or "전체",
        "message": (
            f"매입기록에서 {clean_date} / {clean_product or '전체 상품'} 조건의 {len(matched_rows)}행을 삭제했습니다."
            if matched_rows
            else "조건에 맞는 매입기록이 없습니다."
        ),
        "note": "전표관리/전표상세관리는 중복 방지용으로 유지했습니다. 같은 전표를 다시 넣으려면 전표관리도 별도로 확인해 주세요.",
    }

def setup_purchase_workbook(settings: Settings) -> dict[str, Any]:
    """
    과거에는 시트를 새로 만들고 디자인을 적용했지만,
    이제는 사용자가 업로드한 엑셀 템플릿 구글시트가 정상 연결되어 있는지만 확인한다.
    """
    spreadsheet = _open_purchase_spreadsheet(settings)
    return _check_purchase_template(spreadsheet)

def reprocess_unregistered_items(settings: Settings) -> dict[str, Any]:
    spreadsheet = _open_purchase_spreadsheet(settings)
    worksheets = _load_purchase_template_workbook(spreadsheet)
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
    worksheets = _load_purchase_template_workbook(spreadsheet)
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
