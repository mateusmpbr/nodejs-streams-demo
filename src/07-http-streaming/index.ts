/**
 * Demo 07 — HTTP Streaming
 *
 * HTTP responses are Writable streams. This lets you pipe data directly to the
 * client without buffering the entire response in memory.
 *
 * Endpoints:
 *  • GET /stream — sends 20 chunks with 100 ms delay each (SSE/LLM-style)
 *  • GET /file   — pipes a generated large file directly to the response
 *
 * Usage:
 *   Terminal 1 (server): npx tsx src/07-http-streaming/index.ts --server
 *   Terminal 2 (client): npx tsx src/07-http-streaming/index.ts --client
 */

import * as http from 'node:http';
import { createReadStream, createWriteStream } from 'node:fs';
import { writeFile, unlink } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const PORT = 3700;
const LARGE_FILE = join(tmpdir(), 'streams-demo-large.txt');

// ─── Server ───────────────────────────────────────────────────────────────────
async function startServer(): Promise<void> {
  // Generate large file once at startup
  const line = 'Streaming data chunk — '.repeat(20) + '\n';
  await writeFile(LARGE_FILE, line.repeat(50_000));
  console.log(`[server] Large file ready: ${LARGE_FILE}`);

  const server = http.createServer((req, res) => {
    const url = req.url ?? '/';

    if (url === '/stream') {
      handleStream(res);
    } else if (url === '/file') {
      handleFile(res);
    } else {
      res.writeHead(404);
      res.end('Not found');
    }
  });

  server.listen(PORT, () => {
    console.log(`[server] Listening on http://localhost:${PORT}`);
    console.log('[server] Routes: GET /stream  |  GET /file');
  });

  // Graceful shutdown on Ctrl-C
  process.on('SIGINT', async () => {
    console.log('\n[server] Shutting down…');
    await unlink(LARGE_FILE).catch(() => undefined);
    server.close(() => process.exit(0));
  });
}

function handleStream(res: http.ServerResponse): void {
  res.writeHead(200, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Transfer-Encoding': 'chunked',
    'X-Demo': 'http-streaming',
  });

  let count = 0;
  const TOTAL = 20;

  const timer = setInterval(() => {
    count++;
    const payload = `chunk ${String(count).padStart(2, '0')}/${TOTAL}: ${new Date().toISOString()}\n`;
    res.write(payload);

    if (count >= TOTAL) {
      clearInterval(timer);
      res.end('\n[server] stream complete\n');
    }
  }, 100);

  res.on('close', () => clearInterval(timer));
}

function handleFile(res: http.ServerResponse): void {
  res.writeHead(200, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Transfer-Encoding': 'chunked',
  });

  pipeline(createReadStream(LARGE_FILE), res).catch((err) => {
    if ((err as NodeJS.ErrnoException).code !== 'ERR_STREAM_DESTROYED') {
      console.error('[server] /file pipeline error:', err);
    }
  });
}

// ─── Client ───────────────────────────────────────────────────────────────────
function runClient(): void {
  console.log(`[client] Connecting to http://localhost:${PORT}/stream…\n`);
  const start = Date.now();

  http.get(`http://localhost:${PORT}/stream`, (res) => {
    console.log(`[client] Status: ${res.statusCode}`);
    console.log(`[client] Headers: ${JSON.stringify(res.headers)}\n`);

    res.setEncoding('utf8');

    res.on('data', (chunk: string) => {
      const elapsed = Date.now() - start;
      process.stdout.write(`[+${String(elapsed).padStart(5, ' ')}ms] ${chunk}`);
    });

    res.on('end', () => {
      console.log(`\n[client] Stream ended. Total time: ${Date.now() - start}ms`);
    });

    res.on('error', (err) => console.error('[client] Response error:', err));
  }).on('error', (err) => {
    console.error('[client] Connection error:', err.message);
    console.error(`         Make sure the server is running: npx tsx src/07-http-streaming/index.ts --server`);
    process.exit(1);
  });
}

// ─── Entry point ─────────────────────────────────────────────────────────────
const args = process.argv.slice(2);

if (args.includes('--server')) {
  await startServer();
} else if (args.includes('--client')) {
  runClient();
} else {
  console.log('Usage:');
  console.log('  npx tsx src/07-http-streaming/index.ts --server');
  console.log('  npx tsx src/07-http-streaming/index.ts --client');
  process.exit(0);
}
