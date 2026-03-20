import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');
const electronBinary =
  process.platform === 'win32'
    ? path.join(rootDir, 'node_modules', 'electron', 'dist', 'electron.exe')
    : path.join(rootDir, 'node_modules', 'electron', 'dist', 'electron');
const childEnv = { ...process.env };
delete childEnv.ELECTRON_RUN_AS_NODE;

const electronArgs = ['.'];

if (process.env.ELECTRON_NO_SANDBOX === '1') {
  electronArgs.unshift('--no-sandbox');
}

if (process.env.ELECTRON_DISABLE_GPU === '1') {
  electronArgs.unshift('--disable-gpu');
}

const electron = spawn(electronBinary, electronArgs, {
  cwd: rootDir,
  stdio: 'inherit',
  env: childEnv
});

electron.on('exit', (code) => {
  process.exit(code ?? 0);
});
