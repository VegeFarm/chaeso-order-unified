import math
import unicodedata
from decimal import Decimal, ROUND_HALF_UP

FLOAT_TOLERANCE = 1e-9


def normalize_name(text: str) -> str:
    text = unicodedata.normalize("NFKC", str(text))
    return "".join(text.strip().split())


def is_close(a, b, tol: float = FLOAT_TOLERANCE) -> bool:
    return math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=tol)


def format_number(value) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    if is_close(number, round(number)):
        return f"{int(round(number)):,}"
    return f"{number:,.2f}"


def format_plain_qty(value) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    if is_close(number, round(number)):
        return str(int(round(number)))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def round_half_up(value, nearest=10) -> int:
    if nearest <= 0:
        raise ValueError("nearest must be greater than 0")

    scaled = Decimal(str(value)) / Decimal(str(nearest))
    rounded = scaled.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(rounded * Decimal(str(nearest)))
