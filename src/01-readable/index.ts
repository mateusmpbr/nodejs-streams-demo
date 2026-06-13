/**
 * Demo 01 — Custom Readable Stream
 *
 * A Readable stream is a source of data. Node.js pulls data from it in two modes:
 *
 *  • Flowing mode  — data flows automatically as fast as it's produced. You attach
 *    a 'data' listener (or pipe) to start the flow.
 *
 *  • Paused mode   — data is pulled on demand by calling stream.read() inside the
 *    'readable' event. The stream doesn't push anything until you ask.
 *
 * Internally, _read() is called by the stream machinery when it wants more data.
 * Push null to signal the end of the stream.
 */

import { Readable } from 'node:stream';

class NumberStream extends Readable {
  private current: number;
  private readonly max: number;

  constructor(max: number) {
    super();
    this.current = 1;
    this.max = max;
  }

  _read(): void {
    if (this.current > this.max) {
      this.push(null); // end of stream
      return;
    }
    // Push the number as a string chunk (Buffer by default)
    this.push(String(this.current++));
  }
}

// ─── Section 1: Flowing mode ────────────────────────────────────────────────
console.log('=== Flowing mode ===');
console.log('(data listener starts the flow automatically)\n');

const flowingStream = new NumberStream(5);
const flowingChunks: string[] = [];

flowingStream.on('data', (chunk: Buffer) => {
  flowingChunks.push(chunk.toString());
  process.stdout.write(`chunk received: ${chunk.toString()}\n`);
});

flowingStream.on('end', () => {
  console.log(`\nAll chunks collected: [${flowingChunks.join(', ')}]`);
  runPausedMode();
});

flowingStream.on('error', (err) => console.error('Stream error:', err));

// ─── Section 2: Paused mode ─────────────────────────────────────────────────
function runPausedMode(): void {
  console.log('\n=== Paused mode ===');
  console.log('(data is pulled manually via stream.read() inside "readable" event)\n');

  const pausedStream = new NumberStream(5);
  const pausedChunks: string[] = [];

  pausedStream.on('readable', () => {
    let chunk: Buffer | null;
    // Each number is pushed as a single byte; read(1) pulls exactly one chunk at a time.
    // Without a size argument, read() returns everything currently buffered (multiple chunks
    // concatenated), which obscures the per-chunk structure.
    while ((chunk = pausedStream.read(1)) !== null) {
      pausedChunks.push(chunk.toString());
      process.stdout.write(`pulled chunk: ${chunk.toString()}\n`);
    }
  });

  pausedStream.on('end', () => {
    console.log(`\nAll chunks collected: [${pausedChunks.join(', ')}]`);
  });

  pausedStream.on('error', (err) => console.error('Stream error:', err));
}
