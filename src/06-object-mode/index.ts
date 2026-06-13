/**
 * Demo 06 — Object Mode
 *
 * By default, streams operate on Buffers/strings. Object mode removes that
 * restriction: a stream can push/receive any JavaScript value (objects, arrays,
 * numbers…), and there is no concept of "encoding". The highWaterMark counts
 * objects instead of bytes (default: 16 objects).
 *
 * This pipeline:
 *   ObjectSource → FilterTransform → JSONLinesTransform → FileWritable
 *
 *  1. ObjectSource emits { id, name, value } plain objects
 *  2. FilterTransform keeps only records where value > 50
 *  3. JSONLinesTransform serialises each object to a JSON Lines string
 *  4. The result is written to output.jsonl
 */

import { Readable, Transform, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createWriteStream } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

interface Record {
  id: number;
  name: string;
  value: number;
}

// ─── ObjectSource ─────────────────────────────────────────────────────────────
class ObjectSource extends Readable {
  private index = 0;
  private readonly records: Record[];

  constructor(records: Record[]) {
    super({ objectMode: true });
    this.records = records;
  }

  _read(): void {
    if (this.index >= this.records.length) {
      this.push(null);
      return;
    }
    this.push(this.records[this.index++]);
  }
}

// ─── FilterTransform ──────────────────────────────────────────────────────────
class FilterTransform extends Transform {
  private readonly predicate: (r: Record) => boolean;

  constructor(predicate: (r: Record) => boolean) {
    super({ objectMode: true });
    this.predicate = predicate;
  }

  _transform(record: Record, _encoding: string, callback: () => void): void {
    if (this.predicate(record)) {
      this.push(record);
    }
    callback();
  }
}

// ─── JSONLinesTransform ───────────────────────────────────────────────────────
// Converts object mode input to string output (switches object mode off on the readable side)
class JSONLinesTransform extends Transform {
  constructor() {
    // readableObjectMode: false so the output is a regular string stream
    super({ writableObjectMode: true, readableObjectMode: false });
  }

  _transform(record: Record, _encoding: string, callback: (err: null, data: string) => void): void {
    callback(null, JSON.stringify(record) + '\n');
  }
}

// ─── Sample data ──────────────────────────────────────────────────────────────
const records: Record[] = Array.from({ length: 20 }, (_, i) => ({
  id: i + 1,
  name: `item-${String(i + 1).padStart(2, '0')}`,
  value: Math.floor(Math.random() * 100),
}));

const OUTPUT_PATH = join(tmpdir(), 'streams-demo-output.jsonl');

// ─── Run the pipeline ─────────────────────────────────────────────────────────
console.log('=== Object Mode Pipeline ===\n');
console.log(`Source: ${records.length} records`);
console.log('Filter: value > 50');
console.log(`Output: ${OUTPUT_PATH}\n`);

let passedFilter = 0;

const filterTransform = new FilterTransform((r) => {
  const passes = r.value > 50;
  if (passes) passedFilter++;
  return passes;
});

try {
  await pipeline(
    new ObjectSource(records),
    filterTransform,
    new JSONLinesTransform(),
    createWriteStream(OUTPUT_PATH),
  );

  console.log(`Records emitted:  ${records.length}`);
  console.log(`Records kept:     ${passedFilter} (value > 50)`);
  console.log(`Records filtered: ${records.length - passedFilter}`);
  console.log(`\nOutput written to: ${OUTPUT_PATH}`);
  console.log('[pipeline] Completed successfully.');
} catch (err) {
  console.error('[pipeline] Error:', err);
  process.exit(1);
}
