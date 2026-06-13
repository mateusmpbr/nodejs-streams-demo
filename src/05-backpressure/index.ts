/**
 * Demo 05 — Backpressure
 *
 * Backpressure is the mechanism by which a slow consumer signals a fast producer
 * to slow down, preventing unbounded memory growth.
 *
 * How it works:
 *  • writable.write(chunk) returns false when the internal buffer exceeds
 *    the highWaterMark. This is the signal to STOP writing.
 *  • The writable emits 'drain' when its buffer drops back below the watermark.
 *    This is the signal to RESUME writing.
 *
 * Part A (naïve): ignore the return value of write(). All 500 chunks are queued
 *   in the stream's internal buffer at once, ballooning memory.
 *
 * Part B (correct): respect the boolean return and use the 'drain' event.
 *   At most a handful of chunks are queued at any time.
 *
 * Key metric: "max pending" — the largest number of writes simultaneously in the
 * stream buffer. Part A: 500. Part B: stays near highWaterMark boundary.
 */

import { Writable } from 'node:stream';

const TOTAL_CHUNKS = 500;
const SLOW_WRITE_DELAY_MS = 5;
// 64 KB per chunk; highWaterMark = 2 chunks, so drain fires frequently in Part B
const CHUNK_SIZE = 64 * 1024;
const HIGH_WATER_MARK = 2 * CHUNK_SIZE;

function printMetrics(
  label: string,
  sent: number,
  written: number,
  ms: number,
  maxPending: number,
  peakRss: number,
): void {
  console.log(`\n  ${label}`);
  console.log(`    Chunks sent:    ${sent}`);
  console.log(`    Chunks written: ${written}`);
  console.log(`    Max pending:    ${maxPending}  ← chunks buffered at peak`);
  console.log(`    Elapsed:        ${ms} ms`);
  console.log(`    Peak RSS:       ${peakRss} MB`);
}

// ─── Part A: Naïve (no backpressure) ─────────────────────────────────────────
function runNaive(): Promise<void> {
  return new Promise((resolve) => {
    let sent = 0;
    let written = 0;
    let maxPending = 0;
    let peakRss = 0;
    const start = Date.now();

    const writable = new Writable({
      highWaterMark: HIGH_WATER_MARK,
      write(_chunk, _enc, cb) {
        written++;
        setTimeout(cb, SLOW_WRITE_DELAY_MS);
      },
    });

    writable.on('finish', () => {
      printMetrics(
        'Part A — Naïve (backpressure IGNORED)',
        sent, written, Date.now() - start, maxPending, peakRss,
      );
      resolve();
    });

    // Ignore the false return value — just keep writing all chunks synchronously
    while (sent < TOTAL_CHUNKS) {
      // Allocate a new buffer per write so each queued chunk is distinct memory
      const ok = writable.write(Buffer.alloc(CHUNK_SIZE, sent & 0xff));
      sent++;
      const pending = sent - written;
      if (pending > maxPending) maxPending = pending;
      peakRss = Math.max(peakRss, Math.round(process.memoryUsage().rss / 1024 / 1024));
      void ok; // intentionally ignored
    }
    writable.end();
  });
}

// ─── Part B: Correct (respecting backpressure) ────────────────────────────────
function runCorrect(): Promise<void> {
  return new Promise((resolve) => {
    let sent = 0;
    let written = 0;
    let maxPending = 0;
    let peakRss = 0;
    const start = Date.now();

    const writable = new Writable({
      highWaterMark: HIGH_WATER_MARK,
      write(_chunk, _enc, cb) {
        written++;
        setTimeout(cb, SLOW_WRITE_DELAY_MS);
      },
    });

    writable.on('finish', () => {
      printMetrics(
        'Part B — Correct (backpressure RESPECTED)',
        sent, written, Date.now() - start, maxPending, peakRss,
      );
      resolve();
    });

    function writeNext(): void {
      let canContinue = true;
      while (sent < TOTAL_CHUNKS && canContinue) {
        canContinue = writable.write(Buffer.alloc(CHUNK_SIZE, sent & 0xff));
        sent++;
        const pending = sent - written;
        if (pending > maxPending) maxPending = pending;
        peakRss = Math.max(peakRss, Math.round(process.memoryUsage().rss / 1024 / 1024));
      }

      if (sent < TOTAL_CHUNKS) {
        // Buffer full — pause until writable signals it can accept more
        writable.once('drain', writeNext);
      } else {
        writable.end();
      }
    }

    writeNext();
  });
}

// ─── Main ─────────────────────────────────────────────────────────────────────
console.log('=== Backpressure Demo ===');
console.log(`Sending ${TOTAL_CHUNKS} chunks of ${CHUNK_SIZE / 1024} KB each to a slow writable.`);
console.log(`highWaterMark: ${HIGH_WATER_MARK / 1024} KB (${HIGH_WATER_MARK / CHUNK_SIZE} chunks)\n`);

await runNaive();
await runCorrect();

console.log('\n[done] Notice "max pending" in Part A vs Part B.');
