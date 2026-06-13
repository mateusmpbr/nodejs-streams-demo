/**
 * Demo 04 — stream.pipeline with fs + zlib
 *
 * This demo shows why stream.pipeline() is preferred over manual .pipe() chains.
 *
 * ┌─────────────────────────────────────────────────────────┐
 * │  .pipe() problem                                        │
 * │  If a stream in the middle of a pipe chain emits an     │
 * │  error, only that stream is destroyed. The others keep  │
 * │  running, leaking file handles and memory.              │
 * │                                                         │
 * │  stream.pipeline() problem                              │
 * │  On any error, ALL streams in the chain are destroyed   │
 * │  and the error is forwarded to the callback/promise.    │
 * │  No leaks, no dangling handles.                         │
 * └─────────────────────────────────────────────────────────┘
 *
 * Steps:
 *  1. Generate a >10 MB text file at runtime
 *  2. Compress it with gzip
 *  3. Decompress it and verify byte counts match
 *  4. Clean up temp files
 */

import { createReadStream, createWriteStream } from 'node:fs';
import { writeFile, stat, unlink } from 'node:fs/promises';
import { createGzip, createGunzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

const TMP = tmpdir();
const INPUT_PATH = join(TMP, 'streams-demo-input.txt');
const GZ_PATH = join(TMP, 'streams-demo-output.gz');
const OUTPUT_PATH = join(TMP, 'streams-demo-output.txt');

function formatBytes(bytes: number): string {
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

// ─── Step 1: Generate input file ─────────────────────────────────────────────
async function generateInputFile(): Promise<void> {
  console.log('Step 1: Generating input file (>10 MB)…');
  const line = 'The quick brown fox jumps over the lazy dog. '.repeat(10) + '\n';
  // ~460 bytes per line × 23 000 lines ≈ 10.5 MB
  const content = line.repeat(23_000);
  await writeFile(INPUT_PATH, content, 'utf8');
  const { size } = await stat(INPUT_PATH);
  console.log(`  → Created ${INPUT_PATH} (${formatBytes(size)})`);
}

// ─── Step 2: Compress ─────────────────────────────────────────────────────────
async function compress(): Promise<void> {
  console.log('\nStep 2: Compressing with gzip via stream.pipeline()…');
  await pipeline(
    createReadStream(INPUT_PATH),
    createGzip(),
    createWriteStream(GZ_PATH),
  );
  const { size } = await stat(GZ_PATH);
  console.log(`  → Compressed to ${GZ_PATH} (${formatBytes(size)})`);
}

// ─── Step 3: Decompress and verify ───────────────────────────────────────────
async function decompressAndVerify(): Promise<void> {
  console.log('\nStep 3: Decompressing and verifying integrity…');
  await pipeline(
    createReadStream(GZ_PATH),
    createGunzip(),
    createWriteStream(OUTPUT_PATH),
  );

  const [inputStat, outputStat] = await Promise.all([
    stat(INPUT_PATH),
    stat(OUTPUT_PATH),
  ]);

  console.log(`  → Input size:  ${formatBytes(inputStat.size)}`);
  console.log(`  → Output size: ${formatBytes(outputStat.size)}`);

  if (inputStat.size === outputStat.size) {
    console.log('  ✓ Byte counts match — integrity verified.');
  } else {
    throw new Error('Byte count mismatch after decompression!');
  }
}

// ─── Step 4: Clean up ─────────────────────────────────────────────────────────
async function cleanup(): Promise<void> {
  console.log('\nStep 4: Cleaning up temp files…');
  await Promise.all([
    unlink(INPUT_PATH),
    unlink(GZ_PATH),
    unlink(OUTPUT_PATH),
  ]);
  console.log('  → Done.');
}

// ─── Main ─────────────────────────────────────────────────────────────────────
console.log('=== Pipeline: fs + zlib (compress → decompress → verify) ===\n');
try {
  await generateInputFile();
  await compress();
  await decompressAndVerify();
  await cleanup();
  console.log('\n[pipeline] All steps completed successfully.');
} catch (err) {
  console.error('\n[pipeline] Error:', err);
  process.exit(1);
}
