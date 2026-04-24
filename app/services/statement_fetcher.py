from urllib.parse import urlparse

import requests


class StatementFetchError(Exception):
    pass


ALLOWED_STATEMENT_HOST = "wholesalesales.marketbom.com"
ALLOWED_STATEMENT_PATH_PREFIX = "/statement/"


def _validate_statement_url(url: str) -> str:
    clean_url = (url or "").strip()
    parsed = urlparse(clean_url)

    if parsed.scheme != "https":
        raise StatementFetchError("거래명세서 링크는 https 주소만 사용할 수 있습니다.")

    if parsed.netloc != ALLOWED_STATEMENT_HOST:
        raise StatementFetchError("마켓봄 거래명세서 링크만 사용할 수 있습니다.")

    if not parsed.path.startswith(ALLOWED_STATEMENT_PATH_PREFIX):
        raise StatementFetchError("/statement/ 형식의 거래명세서 링크만 사용할 수 있습니다.")

    return clean_url


def fetch_statement_html_from_url(url: str, max_mb: int = 15) -> bytes:
    statement_url = _validate_statement_url(url)
    max_bytes = max_mb * 1024 * 1024

    try:
        response = requests.get(
            statement_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=(5, 20),
            allow_redirects=True,
            stream=True,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise StatementFetchError("거래명세서 링크를 열 수 없습니다.") from exc

    final_url = response.url or statement_url
    final_parsed = urlparse(final_url)
    if final_parsed.netloc != ALLOWED_STATEMENT_HOST:
        raise StatementFetchError("거래명세서 링크가 허용되지 않은 주소로 이동했습니다.")

    chunks: list[bytes] = []
    total = 0
    try:
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > max_bytes:
                raise StatementFetchError(f"거래명세서 HTML은 {max_mb}MB 이하여야 합니다.")
            chunks.append(chunk)
    finally:
        response.close()

    html_bytes = b"".join(chunks)
    if not html_bytes.strip():
        raise StatementFetchError("거래명세서 링크에서 빈 HTML이 내려왔습니다.")

    return html_bytes
