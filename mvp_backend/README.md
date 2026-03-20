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
