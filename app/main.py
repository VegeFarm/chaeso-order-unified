import shutil
import tempfile
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.config import get_settings
from app.services.pipeline import process_uploaded_files

app = FastAPI(title="채소팜 주문수량확인", version="1.2.0")
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


def _run_job(job_dir: str, html_bytes: bytes, order_bytes: bytes | None) -> None:
    try:
        process_uploaded_files(job_dir, html_bytes, order_bytes)
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


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
            "item_sheet": settings.item_settings_sheet_name,
            "match_sheet": settings.match_rules_sheet_name,
            "price_sheet": settings.price_rules_sheet_name,
            "template_sheet": settings.template_sheet_name,
        },
    )


@app.post("/upload")
async def upload_files(
    background_tasks: BackgroundTasks,
    html_file: UploadFile = File(...),
    order_file: UploadFile | None = File(None),
):
    settings = get_settings()
    _assert_upload_size(html_file, settings.max_upload_mb)

    if not html_file.filename or not html_file.filename.lower().endswith((".html", ".htm")):
        raise HTTPException(status_code=400, detail="거래명세서 파일은 .html 또는 .htm 이어야 합니다.")

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

    html_bytes = _save_bytes(html_file)

    job_dir = tempfile.mkdtemp(prefix="veg-job-")
    job_id = uuid4().hex[:8]

    background_tasks.add_task(_run_job, job_dir, html_bytes, order_bytes)

    if order_attached:
        message = "업로드가 접수되었습니다. 템플릿 시트를 복사해 날짜 시트를 맞추고, 입고열만 반영한 뒤 주문대조/가격계산을 진행하여 텔레그램으로 요약 메시지를 보냅니다."
        mode = "full"
    else:
        message = "업로드가 접수되었습니다. 템플릿 시트를 복사해 날짜 시트를 맞추고, 거래명세서 수량만 집계해서 입고열에 반영합니다."
        mode = "sheet_only"

    return JSONResponse(
        {
            "ok": True,
            "job_id": job_id,
            "mode": mode,
            "message": message,
            "sheets": {
                "item_settings": settings.item_settings_sheet_name,
                "match_rules": settings.match_rules_sheet_name,
                "price_rules": settings.price_rules_sheet_name,
                "template": settings.template_sheet_name,
            },
        }
    )
