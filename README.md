# Node.js Streams — Demo Project

A hands-on collection of seven progressive demos exploring the Node.js Streams API. Each demo is self-contained, uses only Node.js core modules (`node:stream`, `node:fs`, `node:zlib`, `node:http`), and can be run independently.

---

## Prerequisites

- **Node.js 20+** (ESM support, `stream/promises`)
- **npm** (for installing dev dependencies)

```bash
npm install
```

---

## Demos

### 01 — Custom Readable Stream

Implements `NumberStream extends Readable`, a stream that emits numbers 1..N.

Demonstrates the difference between **flowing mode** (data listener) and **paused mode** (manual `.read()` inside the `'readable'` event).

```bash
npx tsx src/01-readable/index.ts
# or
npm run 01
```

---

### 02 — Custom Writable Stream

Implements `LogWritable extends Writable`, which formats each incoming chunk with an ISO timestamp and writes it to stdout.

Shows how `_write()` controls backpressure via its `callback`, and how `_final()` + the `'finish'` event signal completion.

```bash
npx tsx src/02-writable/index.ts
# or
npm run 02
```

---

### 03 — Transform Stream Pipeline

Chains two transforms using `stream.pipeline` from `node:stream/promises`:

1. `UpperCaseTransform` — uppercases every chunk
2. `CSVToJSONTransform` — buffers lines and emits one JSON object per CSV row

Shows why `stream.pipeline` is preferred over manual `.pipe()` (automatic error propagation and cleanup).

```bash
npx tsx src/03-transform/index.ts
# or
npm run 03
```

---

### 04 — fs + zlib Pipeline

Generates a >10 MB text file at runtime, then:

1. Compresses it to `.gz` using `zlib.createGzip()`
2. Decompresses it with `zlib.createGunzip()`
3. Verifies byte counts match (integrity check)
4. Cleans up all temp files

Includes a detailed explanation of why `stream.pipeline` prevents file-handle leaks that `.pipe()` can cause on error.

```bash
npx tsx src/04-pipeline/index.ts
# or
npm run 04
```

---

### 05 — Backpressure

Pits a fast producer against a slow writable (10 ms delay per chunk, 64 KB chunks × 500 rounds).

- **Part A (naïve):** ignores the boolean return value of `.write()` — buffer grows unboundedly
- **Part B (correct):** pauses on `write() === false`, resumes on `'drain'`

Prints a metrics table (chunks sent/written, elapsed ms, peak heap MB) to compare both approaches.

```bash
npx tsx src/05-backpressure/index.ts
# or
npm run 05
```

---

### 06 — Object Mode

Streams don't have to carry bytes. In object mode, any JS value can flow through.

Pipeline:
1. `ObjectSource` — emits `{ id, name, value }` records
2. `FilterTransform` — keeps only records where `value > 50`
3. `JSONLinesTransform` — serialises each object to a JSON Line string
4. Writes to a `.jsonl` file in the system temp directory

```bash
npx tsx src/06-object-mode/index.ts
# or
npm run 06
```

---

### 07 — HTTP Streaming

An HTTP response is a Writable stream. This demo shows two patterns:

- `GET /stream` — sends 20 chunks with 100 ms delay (simulates SSE / LLM token streaming)
- `GET /file` — pipes a large file directly to the response with zero buffering

Run the server in one terminal and the client in another:

```bash
# Terminal 1
npx tsx src/07-http-streaming/index.ts --server
# or: npm run 07:server

# Terminal 2
npx tsx src/07-http-streaming/index.ts --client
# or: npm run 07:client
```

---

## Concept Summary

| Demo | Key Concept | Node.js APIs Used |
|------|-------------|-------------------|
| 01 — Readable | Flowing vs paused mode, `_read()` | `Readable` |
| 02 — Writable | `_write()`, `_final()`, `finish` event | `Writable` |
| 03 — Transform | Chained transforms, `stream.pipeline` | `Transform`, `stream/promises` |
| 04 — Pipeline | `fs` + `zlib`, error propagation, cleanup | `pipeline`, `zlib`, `fs` |
| 05 — Backpressure | `write()` boolean, `drain` event, memory | `Readable`, `Writable` |
| 06 — Object mode | Non-buffer streams, filter/map/serialise | `objectMode`, `Transform` |
| 07 — HTTP | Response as stream, chunked transfer, pipe | `http`, `fs`, `pipeline` |

---

## Project Structure

```
nodejs-streams-demo/
├── package.json
├── tsconfig.json
└── src/
    ├── 01-readable/index.ts
    ├── 02-writable/index.ts
    ├── 03-transform/index.ts
    ├── 04-pipeline/index.ts
    ├── 05-backpressure/index.ts
    ├── 06-object-mode/index.ts
    └── 07-http-streaming/index.ts
```
