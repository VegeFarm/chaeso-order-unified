import os
from collections import defaultdict
from typing import Any

from app.utils.text import format_number, format_plain_qty, is_close, normalize_name, round_half_up


class OrderParseError(Exception):
    pass


def read_order_text(order_bytes: bytes) -> str:
    try:
        return order_bytes.decode("utf-8").strip()
    except UnicodeDecodeError:
        return order_bytes.decode("utf-8-sig", errors="ignore").strip()


def parse_order_lines(order_text: str) -> list[tuple[str, float]]:
    parsed_orders: list[tuple[str, float]] = []

    for raw_line in order_text.splitlines():
        line = raw_line.strip()
        if not line or "-" not in line:
            continue

        try:
            order_name, order_qty_str = [x.strip() for x in line.split("-", 1)]
            order_qty = float(order_qty_str)
        except ValueError:
            continue

        if order_name:
            parsed_orders.append((order_name, order_qty))

    if not parsed_orders:
        raise OrderParseError("주문내역.txt에서 '상품명 - 수량' 형식의 데이터를 찾지 못했습니다.")

    return parsed_orders


def should_exclude_partial_match(search_keyword: str, receipt_name: str) -> bool:
    return search_keyword == "토마토" and "방울" in receipt_name


def sum_matching_receipt_qty(receipt_items: dict[str, float], search_keyword: str) -> float:
    normalized_keyword = normalize_name(search_keyword)

    exact_matches: list[float] = []
    partial_matches: list[float] = []

    for receipt_name, receipt_qty in receipt_items.items():
        normalized_receipt_name = normalize_name(receipt_name)

        if should_exclude_partial_match(search_keyword, receipt_name):
            continue

        if normalized_receipt_name == normalized_keyword:
            exact_matches.append(receipt_qty)
        elif normalized_keyword in normalized_receipt_name:
            partial_matches.append(receipt_qty)

    if exact_matches:
        return sum(exact_matches)
    return sum(partial_matches)


def get_matching_receipt_rows(receipt_rows: list[dict[str, Any]], search_keyword: str) -> list[dict[str, Any]]:
    normalized_keyword = normalize_name(search_keyword)
    exact_matches: list[dict[str, Any]] = []
    partial_matches: list[dict[str, Any]] = []

    for row in receipt_rows:
        receipt_name = row["name"]
        normalized_receipt_name = normalize_name(receipt_name)

        if should_exclude_partial_match(search_keyword, receipt_name):
            continue

        if normalized_receipt_name == normalized_keyword:
            exact_matches.append(row)
        elif normalized_keyword in normalized_receipt_name:
            partial_matches.append(row)

    return exact_matches if exact_matches else partial_matches


def determine_status(expected_qty: float, receipt_qty_sum: float) -> tuple[str, float]:
    if is_close(expected_qty, 0) and is_close(receipt_qty_sum, 0):
        return "일치", 0.0
    if is_close(receipt_qty_sum, 0):
        return "누락", -expected_qty
    if is_close(expected_qty, receipt_qty_sum):
        return "일치", 0.0
    diff = receipt_qty_sum - expected_qty
    return "불일치", diff


def build_match_results(order_items: list[tuple[str, float]], receipt_items: dict[str, float], rules: dict[str, dict[str, Any]]):
    results = []

    for order_name, order_qty in order_items:
        rule = rules.get(order_name, {"keyword": order_name, "multiplier": 1})
        search_keyword = str(rule["keyword"])
        multiplier = float(rule["multiplier"])

        expected_qty = order_qty * multiplier
        receipt_qty_sum = sum_matching_receipt_qty(receipt_items, search_keyword)
        status, diff = determine_status(expected_qty, receipt_qty_sum)

        results.append(
            {
                "주문품명": order_name,
                "주문수량": float(order_qty),
                "원본품명": search_keyword,
                "수량배수": multiplier,
                "예상수량": expected_qty,
                "명세서수량": receipt_qty_sum,
                "차이": diff,
                "상태": status,
            }
        )

    return results


def build_price_results(receipt_rows: list[dict[str, Any]], price_rules: dict[str, dict[str, Any]]):
    aggregated: dict[tuple[str, float, int], float] = defaultdict(float)

    for display_name, rule in price_rules.items():
        keyword = rule.get("keyword", display_name)
        units_per_order = float(rule.get("units_per_order", 1))
        round_to = int(rule.get("round_to", 10))

        if units_per_order <= 0:
            raise ValueError(f"가격설정에서 '{display_name}'의 단위수량은 0보다 커야 합니다.")

        matches = get_matching_receipt_rows(receipt_rows, keyword)
        if not matches:
            continue

        for row in matches:
            cost_price = float(row["unit_price"])
            unit_price = round_half_up(cost_price / units_per_order, round_to)
            key = (display_name, cost_price, unit_price)
            aggregated[key] += float(row["quantity"])

    price_results = []
    for display_name, cost_price, unit_price in aggregated:
        price_results.append(
            {
                "품목": display_name,
                "원가": f"{format_number(cost_price)}원",
                "개당가격": f"{format_number(unit_price)}원",
            }
        )

    price_results.sort(key=lambda x: (x["품목"], x["원가"]))
    return price_results


def build_telegram_message(match_results: list[dict[str, Any]], price_results: list[dict[str, Any]]) -> str:
    total = len(match_results)
    matched = [x for x in match_results if x["상태"] == "일치"]
    missing = [x for x in match_results if x["상태"] == "누락"]
    mismatched = [x for x in match_results if x["상태"] == "불일치"]

    lines = [
        "주문대조 완료",
        f"- 총 {total}개 품목",
        f"- 일치 {len(matched)}개 / 누락 {len(missing)}개 / 불일치 {len(mismatched)}개",
        "",
    ]

    lines.append("[누락]")
    if missing:
        for row in missing:
            lines.append(f"- {row['주문품명']}: {format_plain_qty(row['주문수량'])}개")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[불일치]")
    if mismatched:
        for row in mismatched:
            diff = float(row["차이"])
            lines.append(f"- {row['주문품명']}: {diff:+g}개")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[가격]")
    if price_results:
        for row in price_results:
            lines.append(f"- {row['품목']}: 원가 {row['원가']} / 개당 {row['개당가격']}")
    else:
        lines.append("- 없음")

    return "\n".join(lines)


def save_match_txt(match_results: list[dict[str, Any]], output_path: str) -> None:
    lines = [
        "주문품명\t주문수량\t원본품명\t수량배수\t예상수량\t명세서수량\t차이\t상태"
    ]
    for row in match_results:
        lines.append(
            "\t".join(
                [
                    row["주문품명"],
                    format_plain_qty(row["주문수량"]),
                    row["원본품명"],
                    format_plain_qty(row["수량배수"]),
                    format_plain_qty(row["예상수량"]),
                    format_plain_qty(row["명세서수량"]),
                    f"{float(row['차이']):+g}" if row["상태"] == "불일치" else format_plain_qty(row["차이"]),
                    row["상태"],
                ]
            )
        )
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def save_price_txt(price_results: list[dict[str, Any]], output_path: str) -> None:
    lines = ["품목\t원가\t개당가격"]
    for row in price_results:
        lines.append("\t".join([row["품목"], row["원가"], row["개당가격"]]))
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
