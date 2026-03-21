# MVP Backend

FastAPI backend for file-to-TypeScript generation.

## Supported input files

- `csv`
- `xlsx`
- `xls`
- `pdf`
- `docx`

## What the backend does

- accepts an uploaded file and a target JSON schema
- parses the source file into a normalized internal format
- returns preview data and warnings
- matches source columns to target fields
- generates a TypeScript `transform()` function
- stores users, generations, versions, and artifacts in SQLite
- imports legacy history from `mvp_backend/.runtime/history.db` into `mvp_backend/.runtime/app.sqlite`

## Excel behavior

- every Excel sheet is exposed through `parsed_file.sheets`
- `parsed_file.columns` and `parsed_file.rows` still contain the merged workbook preview
- generation can target a specific sheet through the `selected_sheet` form field
- if no sheet is selected, generation falls back to the merged workbook preview
- if Excel headers are empty, numeric, or `Unnamed:*`, the backend adds a warning

## Main files

- [app.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/app.py): FastAPI entrypoint
- [routes.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/routes.py): API routes
- [parsers.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/parsers.py): file parsing and sheet selection
- [matcher.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/matcher.py): field matching logic
- [generator.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/generator.py): TypeScript and preview generation
- [storage.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/storage.py): uploads, auth, history, SQLite persistence
- [infra/database.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/infra/database.py): SQLite client
- [infra/schema.sql](/abs/path/c:/Users/user/Desktop/123/mvp_backend/infra/schema.sql): database schema

## Run locally

Run from the backend directory because imports in [app.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/app.py) are local-module imports:

```powershell
cd c:\Users\user\Desktop\123\mvp_backend
..\.venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8000 --reload
```

## Run in Docker

From the repository root:

```bash
docker compose up --build
```

The backend container:

- starts `uvicorn app:app --host 0.0.0.0 --port 8000`
- loads mail variables from `mvp_backend/.env`
- persists SQLite/runtime data in `mvp_backend/.runtime`

## GigaChat runtime

The project now uses GigaChat API as the only model runtime.
Set the backend env like this:

```env
TSGEN_MODEL_PROVIDER=gigachat
TSGEN_MODEL_BASE_URL=https://gigachat.devices.sberbank.ru/api/v1
TSGEN_MODEL_NAME=GigaChat-2-Max
TSGEN_GIGACHAT_AUTH_URL=https://ngw.devices.sberbank.ru:9443/api/v2/oauth
TSGEN_GIGACHAT_AUTH_KEY=<authorization_key_from_gigachat_cabinet>
TSGEN_GIGACHAT_SCOPE=GIGACHAT_API_PERS
TSGEN_GIGACHAT_AUTH_SCHEME=Basic
TSGEN_GIGACHAT_CA_BUNDLE=/app/certs/russian_trusted_root_ca_pem.crt
TSGEN_GIGACHAT_SSL_VERIFY=true
TSGEN_MODEL_TIMEOUT_SECONDS=60
```

Notes:

- the backend now fetches and caches the GigaChat access token automatically
- if you already have a short-lived access token, you can set `TSGEN_MODEL_API_KEY` and skip OAuth refresh
- the OAuth token lifetime is 30 minutes according to the official docs
- the backend sends candidate-ranking prompts to `POST /chat/completions`, not the full schema
- no local model container is required anymore
- the backend image now includes the official Russian trusted root CA bundle under `mvp_backend/certs/`
- if OAuth fails with `CERTIFICATE_VERIFY_FAILED`, rebuild the backend image so the new CA bundle is installed
- `TSGEN_GIGACHAT_SSL_VERIFY=false` exists only as a debugging escape hatch and should not be used in normal runtime

Run:

```bash
docker compose up --build
```

### Training runtime

The training/export lifecycle is handled by the backend and writes artifacts under:

```text
mvp_backend/.runtime/training/
```

The backend still supports:

- dataset snapshot export
- training run records
- activation of a completed deployment into runtime

The local trainer path was removed. Training should now be integrated through GigaChat-side fine-tuning jobs.

Health check:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/health -UseBasicParsing
```

Expected response body:

```json
{"status":"ok"}
```

## API

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/generate`
- `GET /api/history/{user_id}`
- `GET /health`

### `POST /api/generate`

`multipart/form-data`

Fields:

- `file`: uploaded source file
- `target_json`: target schema as a JSON string
- `user_id`: optional authorized user id
- `selected_sheet`: optional sheet name for Excel generation
- `keep_guest_file`: optional, defaults to `false`

## Tests

From the repository root:

```powershell
cd c:\Users\user\Desktop\123
.\.venv\Scripts\python.exe -m unittest mvp_backend.test_parsers mvp_backend.test_matcher mvp_backend.test_generate
```

`mvp_backend.test_generate` is a live smoke test and skips automatically if the backend is not running on `127.0.0.1:8000`.
