import logging
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

PARSER_DIR = Path(__file__).resolve().parent / 'parser'
if str(PARSER_DIR) not in sys.path:
    sys.path.insert(0, str(PARSER_DIR))

from ocr_client import get_ocr_service_health
from routes import router
from storage import init_db, ensure_dirs, cleanup_expired_guest_files

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
)

app = FastAPI(title='Generator MVP Backend', version='0.1.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.on_event('startup')
def on_startup() -> None:
    ensure_dirs()
    init_db()
    cleanup_expired_guest_files()


app.include_router(router, prefix='/api')


@app.get('/health')
def health() -> dict[str, object]:
    ocr = get_ocr_service_health()
    return {
        'status': 'ok',
        'ocr_service': ocr,
    }
