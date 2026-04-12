import json

from app.config import get_settings
from app.services.google_sheets import ensure_today_sheet


def main() -> int:
    settings = get_settings()
    result = ensure_today_sheet(settings)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
