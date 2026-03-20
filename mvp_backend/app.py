import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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
def health() -> dict[str, str]:
    return {'status': 'ok'}
