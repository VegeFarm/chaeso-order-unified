from pathlib import Path

from app.config import get_settings
from app.services.google_sheets import update_daily_sheet
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
    save_match_txt,
    save_price_txt,
)
from app.services.rules_loader import load_match_rules, load_price_rules
from app.services.telegram_sender import send_document, send_message


MATCH_FILE_NAME = "대조결과.txt"
PRICE_FILE_NAME = "가격결과.txt"


def process_uploaded_files(job_dir: str, html_bytes: bytes, order_bytes: bytes | None = None) -> dict:
    settings = get_settings()
    work_dir = Path(job_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    statement_data = read_statement_data_from_html_bytes(html_bytes)
    receipt_rows = build_receipt_rows(statement_data)
    receipt_items = build_receipt_items(receipt_rows)
    business_date = parse_business_date(statement_data)
    sheet_title = format_sheet_title(business_date)

    sheet_result = update_daily_sheet(settings, sheet_title, business_date, receipt_rows)

    result = {
        "mode": "sheet_only",
        "business_date": business_date,
        "sheet_title": sheet_title,
        "sheet_result": sheet_result,
        "match_count": 0,
        "price_count": 0,
        "match_file": None,
        "price_file": None,
    }

    if not order_bytes:
        return result

    order_text = read_order_text(order_bytes)
    order_items = parse_order_lines(order_text)

    match_rules = load_match_rules(settings)
    price_rules = load_price_rules(settings)

    match_results = build_match_results(order_items, receipt_items, match_rules)
    price_results = build_price_results(receipt_rows, price_rules)

    match_path = str(work_dir / MATCH_FILE_NAME)
    price_path = str(work_dir / PRICE_FILE_NAME)
    save_match_txt(match_results, match_path)
    save_price_txt(price_results, price_path)

    message = build_telegram_message(match_results, price_results)
    send_message(settings, message)
    send_document(settings, match_path, caption="대조결과")
    send_document(settings, price_path, caption="가격결과")

    result.update(
        {
            "mode": "full",
            "match_count": len(match_results),
            "price_count": len(price_results),
            "match_file": match_path,
            "price_file": price_path,
        }
    )
    return result
