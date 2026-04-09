from pathlib import Path

import requests


class TelegramSendError(Exception):
    pass


def _enabled(settings) -> bool:
    return bool(settings.telegram_bot_token and settings.telegram_chat_id)


def send_message(settings, text: str) -> bool:
    if not _enabled(settings):
        return False

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    response = requests.post(
        url,
        data={
            "chat_id": settings.telegram_chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        },
        timeout=30,
    )
    if not response.ok:
        raise TelegramSendError(f"텔레그램 메시지 전송 실패: {response.text}")
    return True


def send_document(settings, file_path: str, caption: str = "") -> bool:
    if not _enabled(settings):
        return False

    path = Path(file_path)
    if not path.exists():
        raise TelegramSendError(f"전송할 파일이 없습니다: {file_path}")

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendDocument"
    with path.open("rb") as f:
        response = requests.post(
            url,
            data={
                "chat_id": settings.telegram_chat_id,
                "caption": caption,
            },
            files={"document": (path.name, f)},
            timeout=60,
        )
    if not response.ok:
        raise TelegramSendError(f"텔레그램 파일 전송 실패: {response.text}")
    return True
