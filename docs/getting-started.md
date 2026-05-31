# Getting Started

This guide introduces the TypeScript import/export framework and shows how to build common Node and browser pipelines.

The framework is designed for complete import/export workflows, not only parsing. It provides source/sink contracts, codecs, transforms, batching, progress behavior, structured errors, cancellation, and browser/Node adapters.

## Mental Model

Every pipeline is a linear stream:

```text
Source -> Decoder? -> Transform* -> Encoder? -> Batch -> Sink
```

- `Source<T>` produces input items, such as file chunks, browser `Blob` chunks, memory records, or Node stream chunks.
- `Decoder<T>` converts `Uint8Array` chunks into records, for example CSV rows or JSONL values.
- `Transform<I, O>` maps, filters, validates, enriches, or expands records.
- `Encoder<T>` converts records back into `Uint8Array` chunks.
- `Batch<T>` groups items before writing.
- `Sink<T>` consumes batches and writes them to files, streams, downloads, memory, or custom targets.

The core package never imports Node, browser, CSV, JSONL, or business-specific APIs. Those capabilities live in adapter, codec, and transform packages.

## Install

For local repository development:

```bash
npm install
npm run verify
```

For a Node CSV-to-JSONL application after publishing:

```bash
npm install @robbin-io/core @robbin-io/node @robbin-io/codec-csv @robbin-io/codec-jsonl
```

For a browser CSV preview application:

```bash
npm install @robbin-io/core @robbin-io/browser @robbin-io/codec-csv @robbin-io/sink-memory
```

## Minimal Pipeline

```ts
import { pipeline, map } from "@robbin-io/core"

const result = await pipeline()
  .from(source)
  .through(map((record) => record))
  .batch({ size: 1000 })
  .to(sink)
  .run()

if (!result.ok) {
  console.error(result.errors)
}
```

`run()` returns:

```ts
interface RunResult {
  readonly ok: boolean
  readonly metrics: Record<string, number>
  readonly errors: readonly RuntimeError[]
}
```

The pipeline builder carries item types across source, transform, decoder, encoder, batch, and sink boundaries:

```ts
import { map, pipeline, type Sink, type Source } from "@robbin-io/core"

type CsvRow = { id: string; name: string }
type User = { id: number; name: string }

declare const source: Source<CsvRow>
declare const sink: Sink<User>

await pipeline()
  .from(source)
  .through(map((row) => ({ id: Number(row.id), name: row.name.trim() })))
  .to(sink)
  .run()
```

`Decoder<T>` stages accept `Uint8Array` streams. `Encoder<T>` stages produce `Uint8Array` streams. `npm run typecheck:api` protects the public type-inference contracts.

## Node: CSV Import To JSONL Export

```ts
import { pipeline, map, validate } from "@robbin-io/core"
import { csvDecoder } from "@robbin-io/codec-csv"
import { jsonlEncoder } from "@robbin-io/codec-jsonl"
import { fsFileSource, fsFileSink } from "@robbin-io/node"

await pipeline()
  .from(fsFileSource("users.csv"))
  .through(csvDecoder({ header: true }))
  .through(validate((row: any) => Boolean(row.id), "row.id is required"))
  .through(
    map((row: any) => ({
      id: Number(row.id),
      name: String(row.name ?? "").trim()
    }))
  )
  .through(jsonlEncoder())
  .batch({ size: 1000 })
  .to(fsFileSink("users.jsonl", { atomic: true, createParentDirectories: true }))
  .run({ runtime: "node" })
```

Notes:

- `fsFileSource()` reads a Node file as byte chunks.
- `csvDecoder({ header: true })` parses CSV rows into objects.
- `validate()` performs lightweight row validation.
- `map()` performs business transformation.
- `jsonlEncoder()` emits JSONL bytes.
- `fsFileSink(..., { atomic: true })` writes a temporary file and renames it only after a successful pipeline.

## Browser: CSV Upload Preview

```ts
import { blobSource, webWorkerTransform } from "@robbin-io/browser"
import { csvDecoder } from "@robbin-io/codec-csv"
import { pipeline, progressBehavior } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"

const file = input.files![0]
const sink = memorySink<Record<string, string>>()
const progress = progressBehavior()

await pipeline()
  .from(blobSource(file, { chunkSize: 64 * 1024 }))
  .through(csvDecoder({ header: true }))
  .through(
    webWorkerTransform<Record<string, string>, Record<string, string>>(
      () => new Worker(new URL("./normalize.worker.ts", import.meta.url), { type: "module" }),
      { name: "normalize-rows", concurrency: 8, maxPending: 16 }
    )
  )
  .batch({ size: 500 })
  .to(sink)
  .run({
    runtime: "browser",
    behaviors: [progress],
    errorMode: "skip-and-collect"
  })

console.log(progress.getSnapshot())
console.log(sink.getItems().slice(0, 20))
```

The browser CSV decoder uses PapaParse's incremental parser. It parses chunks progressively instead of collecting the entire file into one string.

## Browser Export

Small or medium downloads can use `downloadSink()`:

```ts
import { downloadSink } from "@robbin-io/browser"
import { csvEncoder } from "@robbin-io/codec-csv"
import { pipeline } from "@robbin-io/core"

const sink = downloadSink({ type: "text/csv", name: "download-users" })

await pipeline()
  .from(source)
  .through(csvEncoder({ header: ["id", "name"], escapeFormula: true }))
  .to(sink)
  .run({ runtime: "browser" })

const url = sink.getObjectUrl()
downloadAnchor.href = url
downloadAnchor.download = "users.csv"
sink.dispose()
```

Large browser exports can use `writableStreamSink()` when a native `WritableStream` is available:

```ts
import { writableStreamSink } from "@robbin-io/browser"
import { csvEncoder } from "@robbin-io/codec-csv"
import { pipeline } from "@robbin-io/core"

const stream = await fileHandle.createWritable()

await pipeline()
  .from(source)
  .through(csvEncoder({ header: ["id", "name"], escapeFormula: true }))
  .to(writableStreamSink(stream, { name: "users-export" }))
  .run({ runtime: "browser" })
```

`writableStreamSink()` writes directly to the stream, closes the writer on success, aborts it on failure/cancellation, and tracks bytes/chunks written.

## Field Mapping And Validation

Field mapping:

```ts
import { mapFields } from "@robbin-io/transform-fields"

const normalize = mapFields<{ "User ID": string; profile?: { name?: string } }, { id: string; name: string }>({
  id: { from: "User ID", required: true },
  name: {
    from: "profile.name",
    default: "Unknown",
    transform: (value) => String(value).trim()
  }
})
```

Zod validation:

```ts
import { z } from "zod"
import { validateWithZod } from "@robbin-io/validation-zod"

const userSchema = z.object({
  id: z.coerce.number(),
  name: z.string().min(1)
})

pipeline().through(validateWithZod(userSchema))
```

Validation failures become `RuntimeError` values with structured metadata, suitable for dead-letter output or error reports.

## Progress, Errors, And Cancellation

Low-overhead progress:

```ts
import { pipeline, progressBehavior } from "@robbin-io/core"

const progress = progressBehavior()

await pipeline()
  .from(source)
  .to(sink)
  .run({
    behaviors: [progress]
  })

console.log(progress.getSnapshot())
```

Full runtime observation:

```ts
await pipeline()
  .from(source)
  .to(sink)
  .run({
    onEvent(event) {
      console.log(event.type, event.timestamp)
    }
  })
```

`onEvent` preserves detailed stage, channel, batch flush, and sink write events. It is useful for diagnostics, tracing, and audits, but it is more expensive than the default production path.

Skip bad records and collect errors:

```ts
const result = await pipeline().from(source).through(transform).to(sink).run({
  errorMode: "skip-and-collect"
})

for (const error of result.errors) {
  console.error(error.code, error.stage, error.metadata)
}
```

Cancellation:

```ts
const controller = new AbortController()
const task = pipeline().from(source).to(sink)
const running = task.run({ signal: controller.signal })

controller.abort("user cancelled")

const result = await running
console.log(result.ok) // false
```

## Performance Baseline

Benchmarks are local guidance, not a public SLA. They are useful for capacity estimates and regression checks.

Typical recent results:

| Scenario                            | Approximate result |
| ----------------------------------- | -----------------: |
| 10k records simple map              |            46-50ms |
| 10k records progress behavior       |            58-60ms |
| 10k records full observer           |          225-235ms |
| CSV decode 10k rows                 |            51-52ms |
| CSV decode 100k rows                |          522-537ms |
| 100k memory pipeline map+batch+sink |          520-605ms |
| Large CSV -> JSONL stress path      | about 66k rows/sec |
| Large JSONL -> CSV stress path      | about 82k rows/sec |

Performance guidance:

- The default path is the high-throughput production path.
- `progressBehavior()` is intended for production UI progress.
- `run({ onEvent })` is the full observation path and should be reserved for diagnostics on large files.
- Worker `transfer(input)` is faster than structured clone for transferable `ArrayBuffer` payloads.
- Concurrent transforms help when the transform is truly asynchronous or CPU-heavy; they do not necessarily help immediate synchronous maps.

## Current Quality Gate

Run:

```bash
npm run verify
```

The gate covers formatting, tests, TypeScript checks, API type inference, package builds, package exports, public API surface checks, release docs checks, npm tarball dry-runs, packed-consumer smoke tests, Changesets config checks, Node ESM smoke tests, browser example builds, real browser smokes, and browser worker smokes.

## Remaining Pre-1.0 Work

The framework is close to a controlled beta release, but the following areas still need hardening:

- long-duration memory curves for very large files and lower-memory devices;
- additional File System Access API handle smokes;
- broader connector ecosystem such as Excel, Parquet, databases, and object storage;
- final npm publishing setup, repository environments, branch protection, and release approvals;
- progress semantics cleanup after encoder chunk aggregation.
