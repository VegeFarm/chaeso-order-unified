import json
import re
from typing import Any

from bs4 import BeautifulSoup

from app.services.business_dates import format_sheet_title_from_business_date, resolve_business_date


class HtmlStatementError(Exception):
    pass


def read_statement_data_from_html_bytes(html_bytes: bytes) -> dict[str, Any]:
    try:
        html_text = html_bytes.decode("utf-8")
    except UnicodeDecodeError:
        html_text = html_bytes.decode("utf-8-sig", errors="ignore")

    soup = BeautifulSoup(html_text, "html.parser")
    input_tag = soup.find("input", {"name": "statement"})
    if not input_tag:
        raise HtmlStatementError("HTML 안에서 name='statement' 값을 찾지 못했습니다. 렌더링 완료된 거래명세서 HTML인지 확인해 주세요.")

    raw_value = input_tag.get("value", "")
    if not raw_value:
        raise HtmlStatementError("statement 값이 비어 있습니다.")

    try:
        statement_data = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise HtmlStatementError("statement JSON 파싱에 실패했습니다.") from exc

    validate_statement_data(statement_data)
    return statement_data


def validate_statement_data(statement_data: dict[str, Any]) -> None:
    raw_date = str(statement_data.get("slip", {}).get("date", "")).strip()
    if not raw_date:
        raise HtmlStatementError("거래명세서 안에서 거래일자를 찾지 못했습니다.")

    receipt_rows = build_receipt_rows(statement_data)
    if not receipt_rows:
        raise HtmlStatementError("거래명세서 안에서 품목 행을 1개 이상 찾지 못했습니다.")


def parse_business_date(statement_data: dict[str, Any], timezone_str: str = "Asia/Seoul") -> str:
    raw_date = str(statement_data.get("slip", {}).get("date", "")).strip()
    return resolve_business_date(raw_date, timezone_str)


def format_sheet_title(business_date: str) -> str:
    return format_sheet_title_from_business_date(business_date)


def build_receipt_rows(statement_data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for item in statement_data.get("goods", []):
        if item.get("row_type") != "goods":
            continue

        name = str(item.get("name", "")).strip()
        if not name:
            continue

        try:
            quantity = float(item.get("quantity", 0))
            unit_price = float(item.get("unit_price", 0))
            sum_amount = float(item.get("sum_amount", 0))
        except (TypeError, ValueError):
            continue

        if quantity < 0:
            continue

        rows.append(
            {
                "name": name,
                "quantity": quantity,
                "unit": str(item.get("unit", "")).strip(),
                "unit_price": unit_price,
                "sum_amount": sum_amount,
            }
        )

    return rows


def build_receipt_items(receipt_rows: list[dict[str, Any]]) -> dict[str, float]:
    receipt_items: dict[str, float] = {}
    for row in receipt_rows:
        receipt_items[row["name"]] = receipt_items.get(row["name"], 0.0) + float(row["quantity"])
    return receipt_items
