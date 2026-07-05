from pathlib import Path

from app.config import get_settings
from app.services.google_sheets import ensure_today_sheet, update_daily_sheet
from app.services.html_parser import (
    build_receipt_items,
    build_receipt_rows,
    format_sheet_title,
    parse_business_date,
    read_statement_data_from_html_bytes,
)
from app.services.order_compare import (
    build_match_results,
    build_price_results,
    build_telegram_message,
    parse_order_lines,
    read_order_text,
)
from app.services.rules_loader import load_match_rules, load_price_rules
from app.services.telegram_sender import send_message


def process_uploaded_files(job_dir: str, html_bytes: bytes, order_bytes: bytes | None = None) -> dict:
    settings = get_settings()
    work_dir = Path(job_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    today_sheet_result = ensure_today_sheet(settings)

    statement_data = read_statement_data_from_html_bytes(html_bytes)
    receipt_rows = build_receipt_rows(statement_data)
    receipt_items = build_receipt_items(receipt_rows)
    business_date = parse_business_date(statement_data, settings.business_timezone)
    sheet_title = format_sheet_title(business_date)

    sheet_result = update_daily_sheet(settings, sheet_title, business_date, receipt_rows)

    result = {
        "mode": "sheet_only",
        "business_date": business_date,
        "sheet_title": sheet_title,
        "today_sheet_result": today_sheet_result,
        "sheet_result": sheet_result,
        "match_count": 0,
        "price_count": 0,
    }

    if not order_bytes:
        return result

    order_text = read_order_text(order_bytes)
    order_items = parse_order_lines(order_text)

    match_rules = load_match_rules(settings)
    price_rules = load_price_rules(settings)

    match_results = build_match_results(order_items, receipt_items, match_rules)
    price_results = build_price_results(receipt_rows, price_rules)

    message = build_telegram_message(match_results, price_results)
    send_message(settings, message)

    result.update(
        {
            "mode": "full",
            "match_count": len(match_results),
            "price_count": len(price_results),
        }
    )
    return result
