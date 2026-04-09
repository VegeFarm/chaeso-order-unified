import json
import re
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup


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
        raise HtmlStatementError("HTML 파일에서 name='statement' 값을 찾지 못했습니다.")

    raw_value = input_tag.get("value", "")
    if not raw_value:
        raise HtmlStatementError("statement 값이 비어 있습니다.")

    try:
        return json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise HtmlStatementError("statement JSON 파싱에 실패했습니다.") from exc


def parse_business_date(statement_data: dict[str, Any]) -> str:
    raw_date = str(statement_data.get("slip", {}).get("date", "")).strip()
    if not raw_date:
        return datetime.now().strftime("%Y-%m-%d")

    digits = re.sub(r"[^0-9]", "", raw_date)
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"

    return raw_date


def format_sheet_title(business_date: str) -> str:
    digits = re.sub(r"[^0-9]", "", business_date)
    if len(digits) >= 8:
        month = int(digits[4:6])
        day = digits[6:8]
        return f"{month}.{day}"
    return business_date


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
