# TSGen Workspace

Monorepo with a desktop client and a FastAPI backend for file-to-TypeScript generation.

## Structure

```text
123/
|- front/                  React + Vite + Electron desktop client
|- mvp_backend/            FastAPI backend and parsing/generation pipeline
|  |- infra/               SQLite client, shared schema, password hashing
|  |- parser/              PDF/DOCX parsing helpers
|  |- app.py               FastAPI entrypoint
|  |- routes.py            API routes
|  |- parsers.py           File parsing and sheet selection logic
|  |- matcher.py           Field matching logic
|  |- generator.py         TypeScript + preview generation
|  |- storage.py           Uploads, auth, history, DB persistence
|- README.md
```

## What changed

- The backend now uses a richer SQLite schema adapted from the `zov` project.
- Users, generations, versions, mapping cache, and generation artifacts are stored in `mvp_backend/.runtime/app.sqlite`.
- Legacy history from `mvp_backend/.runtime/history.db` is imported into the new database on startup.
- Desktop auth now works through backend endpoints instead of local random IDs.
- Excel generation can target the currently selected sheet in the UI.

## Main flows

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/generate`
- `GET /api/history/{user_id}`
- `GET /health`

## Run backend

From [mvp_backend/app.py](/abs/path/c:/Users/user/Desktop/123/mvp_backend/app.py):

```powershell
cd c:\Users\user\Desktop\123\mvp_backend
..\.venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8000 --reload
```

## Run with Docker Compose

From the repository root:

```bash
docker compose up --build
```

Available endpoints:

- frontend: `http://127.0.0.1:8080`
- backend API: `http://127.0.0.1:8000`

Notes:

- backend reads mail settings from `mvp_backend/.env`
- SQLite data persists in `mvp_backend/.runtime`
- frontend proxies `/api` and `/health` to the backend container through nginx

## Run desktop client

From [front/package.json](/abs/path/c:/Users/user/Desktop/123/front/package.json):

```powershell
cd c:\Users\user\Desktop\123\front
npm install
npm run electron:dev
```

## Verification

Backend tests:

```powershell
cd c:\Users\user\Desktop\123
.\.venv\Scripts\python.exe -m unittest mvp_backend.test_parsers mvp_backend.test_matcher mvp_backend.test_generate
```

`mvp_backend.test_generate` is a live smoke test. It is skipped automatically if the backend is not running on `127.0.0.1:8000`.

Frontend build:

```powershell
cd c:\Users\user\Desktop\123\front
npm run build
```

Backend health:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/health -UseBasicParsing
```
