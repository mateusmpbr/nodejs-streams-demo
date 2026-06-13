/**
 * Demo 02 — Custom Writable Stream
 *
 * A Writable stream is a destination for data. You implement _write() to define
 * what happens when a chunk arrives. The callback must be called when the chunk
 * has been fully processed — this is what enables backpressure (the stream won't
 * accept the next chunk until you signal you're ready).
 *
 * Key events:
 *  • 'finish' — fired after .end() and all pending _write() calls complete
 *  • 'error'  — fired on any unhandled error inside _write()
 *
 * .write(chunk) returns false when the internal buffer is full (highWaterMark
 * exceeded), signalling the producer to pause. .end() flushes and closes.
 */

import { Writable } from 'node:stream';

class LogWritable extends Writable {
  private lineCount = 0;

  _write(chunk: Buffer, _encoding: BufferEncoding, callback: () => void): void {
    this.lineCount++;
    const timestamp = new Date().toISOString();
    const message = chunk.toString().trim();
    process.stdout.write(`[${timestamp}] #${this.lineCount} ${message}\n`);
    callback(); // signal that this chunk has been processed
  }

  _final(callback: () => void): void {
    process.stdout.write(`[LogWritable] stream closed after ${this.lineCount} line(s)\n`);
    callback();
  }
}

// ─── Drive the writable ──────────────────────────────────────────────────────
const log = new LogWritable();

log.on('finish', () => {
  console.log('\n[finish event] All writes flushed and stream ended.');
});

log.on('error', (err) => {
  console.error('[error event]', err);
});

const messages = [
  'Server started on port 3000',
  'Request received: GET /',
  'Database query took 12ms',
  'Response sent: 200 OK',
  'Connection closed',
];

console.log('=== Writing to LogWritable ===\n');

for (const msg of messages) {
  log.write(msg);
}

log.end(); // flush + close; triggers 'finish' after _final()
