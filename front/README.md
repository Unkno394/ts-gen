# TSGen Desktop Client

Electron desktop client built with React and Vite.

## What it does

- lets the user register or log in through the backend
- uploads CSV, XLSX, XLS, PDF, and DOCX files
- previews spreadsheet data, including separate Excel sheet tabs
- sends the active Excel sheet to the backend for generation
- shows generated TypeScript, preview JSON, warnings, and saved history

## Runtime assumptions

- the backend must be available at `http://127.0.0.1:8000`
- in Electron mode the frontend talks to that backend directly
- in plain Vite dev mode, `/api` and `/health` are proxied through [vite.config.ts](/abs/path/c:/Users/user/Desktop/123/front/vite.config.ts)

## Main commands

From [package.json](/abs/path/c:/Users/user/Desktop/123/front/package.json):

```powershell
cd c:\Users\user\Desktop\123\front
npm install
npm run electron:dev
```

Useful alternatives:

```powershell
npm run build
npm run dev
npm run electron:start
```

## Dev flow

- `npm run electron:dev` starts a Vite dev server on the first free port beginning from `5180`
- the Electron launcher passes that dev-server URL into [electron/main.cjs](/abs/path/c:/Users/user/Desktop/123/front/electron/main.cjs)
- the client uses [api.ts](/abs/path/c:/Users/user/Desktop/123/front/src/lib/api.ts) for backend requests

## Important files

- [src/App.tsx](/abs/path/c:/Users/user/Desktop/123/front/src/App.tsx): app shell and auth/workspace switching
- [src/components/AuthScreen.tsx](/abs/path/c:/Users/user/Desktop/123/front/src/components/AuthScreen.tsx): register/login screen
- [src/components/Workspace.tsx](/abs/path/c:/Users/user/Desktop/123/front/src/components/Workspace.tsx): upload, preview, generation, history UI
- [src/lib/api.ts](/abs/path/c:/Users/user/Desktop/123/front/src/lib/api.ts): backend API calls and timeouts
- [src/styles.css](/abs/path/c:/Users/user/Desktop/123/front/src/styles.css): desktop UI styling
- [electron/main.cjs](/abs/path/c:/Users/user/Desktop/123/front/electron/main.cjs): Electron main process
- [scripts/electron-dev.mjs](/abs/path/c:/Users/user/Desktop/123/front/scripts/electron-dev.mjs): dev launcher

## Verification

```powershell
cd c:\Users\user\Desktop\123\front
npm run build
```
