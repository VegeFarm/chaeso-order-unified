import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def _parse_date_text(raw_date: str) -> date | None:
    digits = re.sub(r"[^0-9]", "", str(raw_date or "").strip())
    if len(digits) < 8:
        return None
    return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))


def _now_in_timezone(timezone_str: str) -> datetime:
    return datetime.now(ZoneInfo(timezone_str))


def apply_saturday_skip(target_date: date) -> date:
    # 토요일은 작업하지 않으므로 토요일 날짜는 일요일로 보정한다.
    if target_date.weekday() == 5:
        return target_date + timedelta(days=1)
    return target_date


def resolve_business_date(raw_date: str, timezone_str: str) -> str:
    parsed = _parse_date_text(raw_date)
    if parsed is None:
        parsed = _now_in_timezone(timezone_str).date()
    adjusted = apply_saturday_skip(parsed)
    return adjusted.strftime("%Y-%m-%d")


def format_sheet_title_from_business_date(business_date: str) -> str:
    parsed = _parse_date_text(business_date)
    if parsed is None:
        return business_date
    return f"{parsed.month}.{parsed.day:02d}"


def resolve_auto_sheet_date(now: datetime) -> date:
    return apply_saturday_skip(now.date())
