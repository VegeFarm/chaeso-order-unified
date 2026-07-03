import shutil
import tempfile
from uuid import uuid4

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.config import get_settings
from app.services.html_parser import HtmlStatementError, read_statement_data_from_html_bytes
from app.services.pipeline import process_uploaded_files
from app.services.purchase_records import (
    PurchaseRecordError,
    apply_product_renames,
    delete_purchase_records,
    reprocess_unregistered_items,
)
from app.services.statement_fetcher import StatementFetchError, fetch_statement_html_from_url

app = FastAPI(title="채소팜 통합 처리기", version="1.5.0")
templates = Jinja2Templates(directory="app/templates")


def _assert_upload_size(file: UploadFile, max_mb: int) -> None:
    file.file.seek(0, 2)
    size = file.file.tell()
    file.file.seek(0)
    if size > max_mb * 1024 * 1024:
        raise HTTPException(status_code=400, detail=f"파일 크기는 {max_mb}MB 이하여야 합니다.")


def _save_bytes(upload: UploadFile) -> bytes:
    upload.file.seek(0)
    return upload.file.read()


def _validate_statement_html(html_bytes: bytes) -> None:
    try:
        read_statement_data_from_html_bytes(html_bytes)
    except HtmlStatementError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/health")
def health_check():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    settings = get_settings()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "app_name": app.title,
            "inventory_sheet_configured": bool(settings.spreadsheet_id),
            "purchase_sheet_configured": bool(settings.purchase_spreadsheet_id),
            "item_sheet": settings.item_settings_sheet_name,
            "match_sheet": settings.match_rules_sheet_name,
            "price_sheet": settings.price_rules_sheet_name,
            "template_sheet": settings.template_sheet_name,
        },
    )


@app.post("/upload")
async def upload_files(
    statement_url: str = Form(""),
    html_file: UploadFile | None = File(None),
    order_file: UploadFile | None = File(None),
):
    settings = get_settings()

    clean_statement_url = (statement_url or "").strip()
    source = "file"

    if clean_statement_url:
        try:
            html_bytes = fetch_statement_html_from_url(clean_statement_url, settings.max_upload_mb)
        except StatementFetchError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        source = "url"
    else:
        if not html_file or not getattr(html_file, "filename", None):
            raise HTTPException(status_code=400, detail="거래명세서 HTML 파일을 올리거나 거래명세서 링크를 입력해 주세요.")

        _assert_upload_size(html_file, settings.max_upload_mb)

        if not html_file.filename.lower().endswith((".html", ".htm")):
            raise HTTPException(status_code=400, detail="거래명세서 파일은 .html 또는 .htm 이어야 합니다.")

        html_bytes = _save_bytes(html_file)

    if not html_bytes.strip():
        raise HTTPException(status_code=400, detail="거래명세서 HTML이 비어 있습니다.")

    _validate_statement_html(html_bytes)

    order_bytes: bytes | None = None
    order_attached = bool(order_file and getattr(order_file, "filename", None))
    if order_attached:
        _assert_upload_size(order_file, settings.max_upload_mb)
        if not order_file.filename.lower().endswith((".txt", ".csv")):
            raise HTTPException(status_code=400, detail="주문내역 파일은 .txt 또는 .csv 이어야 합니다.")
        order_bytes = _save_bytes(order_file)
        if not order_bytes.strip():
            order_bytes = None
            order_attached = False

    job_dir = tempfile.mkdtemp(prefix="veg-job-")
    source_text = "거래명세서 링크" if source == "url" else "거래명세서 HTML 파일"

    try:
        result = process_uploaded_files(job_dir, html_bytes, order_bytes)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"처리 중 오류: {exc}") from exc
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)

    result.update(
        {
            "ok": True,
            "source": source,
            "message": (
                f"{source_text} 처리가 완료되었습니다. 아래 purchase_result.inserted_rows / "
                "unregistered_rows / error 값을 확인해 주세요."
            ),
            "sheets": {
                "재고파악_시트": {
                    "template": settings.template_sheet_name,
                    "item_settings": settings.item_settings_sheet_name,
                    "match_rules": settings.match_rules_sheet_name,
                    "price_rules": settings.price_rules_sheet_name,
                },
                "매입단가_시트": {
                    "configured": bool(settings.purchase_spreadsheet_id),
                    "required_env": "PURCHASE_SPREADSHEET_ID",
                },
            },
        }
    )
    return JSONResponse(result)


@app.post("/purchase/reprocess-unregistered")
def purchase_reprocess_unregistered():
    settings = get_settings()
    try:
        result = reprocess_unregistered_items(settings)
    except PurchaseRecordError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"미등록상품 재처리 중 오류: {exc}") from exc
    return JSONResponse(result)


@app.post("/purchase/apply-renames")
def purchase_apply_renames():
    settings = get_settings()
    try:
        result = apply_product_renames(settings)
    except PurchaseRecordError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"상품명 일괄변경 중 오류: {exc}") from exc
    return JSONResponse(result)


@app.post("/purchase/delete-records")
def purchase_delete_records(
    delete_date: str = Form(""),
    delete_product: str = Form(""),
):
    settings = get_settings()
    try:
        result = delete_purchase_records(settings, delete_date, delete_product)
    except PurchaseRecordError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"매입기록 삭제 중 오류: {exc}") from exc
    return JSONResponse(result)
