# Browser API

`@robbin-io/browser` provides browser-specific adapters such as `Blob`/`File` sources, download sinks, `WritableStream` sinks, and Web Worker transforms.

## File / Blob Source

```ts
import { fileSource } from "@robbin-io/browser"

pipeline().from(fileSource(file))
```

Use `blobSource(blob, { chunkSize })` when you need predictable browser read chunking instead of the browser's default `Blob.stream()` chunk size. Blob and file sources emit byte metadata on record events for chunks they read.

## Download Sink

```ts
import { downloadSink } from "@robbin-io/browser"

const sink = downloadSink({ type: "text/csv", name: "users-download" })
await pipeline().to(sink).run()
const url = sink.getObjectUrl()
```

`downloadSink()` stores chunks in memory and is intended for small to medium browser downloads. It records bytes written, revokes stale object URLs before later writes, and revokes the active object URL on pipeline close. Call `sink.dispose()` when the UI no longer needs the generated Blob data.

## WritableStream Sink

```ts
import { writableStreamSink } from "@robbin-io/browser"

const stream = await fileHandle.createWritable()
await pipeline().to(writableStreamSink(stream)).run()
```

Use `writableStreamSink()` for large browser exports when a native `WritableStream` is available, for example File System Access API handles, StreamSaver-style integrations, or test/runtime-provided streams. It writes each batch item directly to the stream, tracks bytes/chunks written, closes the writer on success, and aborts it when the pipeline fails or is cancelled.

The real-browser smoke for this path is:

```bash
npm run smoke:browser-writable-stream
```

## Web Worker Transform

Main thread:

```ts
import { webWorkerTransform } from "@robbin-io/browser"

const normalizeRows = webWorkerTransform(() => new Worker(new URL("./worker.ts", import.meta.url), { type: "module" }))
```

Worker:

```ts
import { exposeWorkerTransform } from "@robbin-io/browser"

exposeWorkerTransform((row) => row)
```

Workers can opt into cooperative cancellation by accepting the second handler argument:

```ts
exposeWorkerTransform(async (row, { signal }) => {
  if (signal.aborted) return undefined
  return row
})
```

The main-thread adapter sends `io:cancel` when a request times out or when the transform closes. Handlers that ignore the signal remain compatible.

For worker-heavy pipelines, prefer `progressBehavior()` or record/batch behavior hooks for UI progress. Passing `run({ onEvent })` enables full internal event observation and can dominate worker transform throughput for large files.

For browser CSV imports, a production-oriented path is:

```text
blobSource({ chunkSize }) -> csvDecoder() -> webWorkerTransform() -> sink
```

Keep row progress on `progressBehavior()` and reserve `run({ onEvent })` for diagnostics.

The browser CSV smoke fixture supports larger local checks:

```bash
npm run smoke:browser-csv-large
```

This is a large-ish correctness and boundary smoke, not a release SLA.
