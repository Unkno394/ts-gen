import { spawn } from 'node:child_process';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');
const electronBinary =
  process.platform === 'win32'
    ? path.join(rootDir, 'node_modules', 'electron', 'dist', 'electron.exe')
    : path.join(rootDir, 'node_modules', 'electron', 'dist', 'electron');
function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function findOpenPort(startPort) {
  return new Promise((resolve, reject) => {
    const tryPort = (port) => {
      const server = net.createServer();
      server.unref();
      server.on('error', () => tryPort(port + 1));
      server.listen({ host: '127.0.0.1', port }, () => {
        const address = server.address();
        server.close(() => {
          if (address && typeof address === 'object') {
            resolve(address.port);
          } else {
            reject(new Error('Failed to resolve open port'));
          }
        });
      });
    };
    tryPort(startPort);
  });
}

async function waitForServer(url, attempts = 120) {
  for (let i = 0; i < attempts; i += 1) {
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch {}
    await wait(500);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

const vitePort = await findOpenPort(5180);
const viteUrl = `http://127.0.0.1:${vitePort}`;
const childEnv = { ...process.env };
delete childEnv.ELECTRON_RUN_AS_NODE;
const electronArgs = ['.'];

if (process.env.ELECTRON_NO_SANDBOX === '1') {
  electronArgs.unshift('--no-sandbox');
}

if (process.env.ELECTRON_DISABLE_GPU === '1') {
  electronArgs.unshift('--disable-gpu');
}

const vite = spawn('npm', ['run', 'dev', '--', '--host', '127.0.0.1', '--port', String(vitePort), '--strictPort'], {
  cwd: rootDir,
  stdio: 'inherit',
  shell: process.platform === 'win32',
  env: childEnv,
});

let shuttingDown = false;

function shutdown(code = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  vite.kill('SIGTERM');
  process.exit(code);
}

process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));

try {
  await waitForServer(viteUrl);
  const electron = spawn(electronBinary, electronArgs, {
    cwd: rootDir,
    stdio: 'inherit',
    env: { ...childEnv, VITE_DEV_SERVER_URL: viteUrl }
  });

  electron.on('exit', (code) => {
    shutdown(code ?? 0);
  });
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  shutdown(1);
}
