/**
 * Demo 03 — Transform Stream Pipeline
 *
 * A Transform is both Readable and Writable. It receives chunks on the writable
 * side, processes them, and pushes results on the readable side.
 *
 * This demo chains two transforms:
 *   1. UpperCaseTransform  — converts each chunk to upper case
 *   2. CSVToJSONTransform  — accumulates lines and emits one JSON object per row
 *
 * Pipeline:
 *   csvSource → UpperCaseTransform → CSVToJSONTransform → process.stdout
 *
 * We use stream.pipeline (node:stream/promises) instead of .pipe() so that
 * errors in any stage are propagated and all streams are destroyed automatically.
 */

import { Transform, Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

// ─── UpperCaseTransform ──────────────────────────────────────────────────────
class UpperCaseTransform extends Transform {
  _transform(chunk: Buffer, _encoding: BufferEncoding, callback: (err?: Error | null, data?: Buffer) => void): void {
    callback(null, Buffer.from(chunk.toString().toUpperCase()));
  }
}

// ─── CSVToJSONTransform ──────────────────────────────────────────────────────
class CSVToJSONTransform extends Transform {
  private buffer = '';
  private headers: string[] = [];
  private isFirstLine = true;

  _transform(chunk: Buffer, _encoding: BufferEncoding, callback: () => void): void {
    this.buffer += chunk.toString();
    const lines = this.buffer.split('\n');
    // Keep the last (possibly incomplete) line in the buffer
    this.buffer = lines.pop() ?? '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      if (this.isFirstLine) {
        this.headers = trimmed.split(',').map((h) => h.trim());
        this.isFirstLine = false;
        continue;
      }

      const values = trimmed.split(',').map((v) => v.trim());
      const obj: Record<string, string> = {};
      this.headers.forEach((header, i) => {
        obj[header] = values[i] ?? '';
      });
      this.push(JSON.stringify(obj) + '\n');
    }
    callback();
  }

  _flush(callback: () => void): void {
    // Process any remaining buffered data
    const trimmed = this.buffer.trim();
    if (trimmed && !this.isFirstLine) {
      const values = trimmed.split(',').map((v) => v.trim());
      const obj: Record<string, string> = {};
      this.headers.forEach((header, i) => {
        obj[header] = values[i] ?? '';
      });
      this.push(JSON.stringify(obj) + '\n');
    }
    callback();
  }
}

// ─── CSV source ──────────────────────────────────────────────────────────────
const csvData = `id,name,role,city
1,Alice,engineer,São Paulo
2,Bob,designer,Rio de Janeiro
3,Carol,manager,Belo Horizonte
4,Dave,engineer,Curitiba
`;

const csvSource = Readable.from([csvData]);

// ─── Run the pipeline ────────────────────────────────────────────────────────
console.log('=== CSV → UpperCase → JSON Lines ===\n');

try {
  await pipeline(
    csvSource,
    new UpperCaseTransform(),
    new CSVToJSONTransform(),
    process.stdout,
  );
  console.log('\n[pipeline] Completed successfully.');
} catch (err) {
  console.error('[pipeline] Error:', err);
  process.exit(1);
}
