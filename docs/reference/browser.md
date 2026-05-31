# Browser API / Browser API 参�?
`@robbin-io/browser` provides browser-specific adapters such as `Blob`/`File` sources, download sinks, and Web Worker transforms.

`@robbin-io/browser` 提供浏览器专属适配器，例如 `Blob`/`File` source、下�?sink �?Web Worker transform�?
## File / Blob Source

```ts
import { fileSource } from "@robbin-io/browser"

pipeline().from(fileSource(file))
```

Use `blobSource(blob, { chunkSize })` when you need predictable browser read chunking instead of the browser's default `Blob.stream()` chunk size. Blob/file sources emit byte metadata on record events for chunks they read.

需要可预测的浏览器读取切块时，使用 `blobSource(blob, { chunkSize })`，而不是依赖浏览器默认�?`Blob.stream()` chunk size。Blob/file source 会在读取 chunk 时通过 record event 发出 bytes metadata�?
## Download Sink

```ts
import { downloadSink } from "@robbin-io/browser"

const sink = downloadSink("text/csv")
await pipeline().to(sink).run()
const url = sink.getObjectUrl()
```

`downloadSink()` stores chunks in memory and is intended for small to medium browser downloads. It records bytes written, revokes stale object URLs before later writes, and revokes the active object URL on pipeline close. Call `sink.dispose()` when the UI no longer needs the generated Blob data.

`downloadSink()` 会把 chunk 保存在内存中，适合中小型浏览器下载。它会记录写�?bytes，在后续写入�?revoke 过期 object URL，并�?pipeline close �?revoke 当前 object URL。UI 不再需要生成的 Blob 数据时，调用 `sink.dispose()` 释放数据�?
## WritableStream Sink / WritableStream Sink

```ts
import { writableStreamSink } from "@robbin-io/browser"

const stream = await fileHandle.createWritable()
await pipeline().to(writableStreamSink(stream)).run()
```

Use `writableStreamSink()` for large browser exports when a native `WritableStream` is available, for example File System Access API handles, StreamSaver-style integrations, or test/runtime-provided streams. It writes each batch item directly to the stream, tracks bytes/chunks written, closes the writer on success, and aborts it when the pipeline fails or is cancelled.

在可获得原生 `WritableStream` 时，使用 `writableStreamSink()` 处理较大的浏览器导出，例�?File System Access API handle、StreamSaver 类集成，或测�?运行时提供的 stream。它会把每个 batch item 直接写入 stream，记录写�?bytes/chunks，成功时 close writer，pipeline 失败或取消时 abort writer�?
The real-browser smoke for this path is:

```bash
npm run smoke:browser-writable-stream
```

## Web Worker Transform

Main thread:

主线程：

```ts
import { webWorkerTransform } from "@robbin-io/browser"

const normalizeRows = webWorkerTransform(() => new Worker(new URL("./worker.ts", import.meta.url), { type: "module" }))
```

For worker-heavy pipelines, prefer `progressBehavior()` or record/batch behavior hooks for UI progress. Passing `run({ onEvent })` enables full internal event observation and can dominate worker transform throughput for large files.

Use `concurrency` to control the pipeline transform concurrency. Use `maxPending` to cap the number of worker requests queued inside the adapter; when the cap is reached, the transform waits for a worker response instead of posting more messages. Use `transfer(input)` for transferable payloads such as `ArrayBuffer` when ownership transfer is acceptable.

For browser CSV imports, a production-oriented path is `blobSource({ chunkSize }) -> csvDecoder() -> webWorkerTransform() -> sink`. Keep row progress on `progressBehavior()` and reserve `run({ onEvent })` for diagnostics, because full observer mode emits internal channel events.

浏览�?CSV 导入的推荐生产路径是 `blobSource({ chunkSize }) -> csvDecoder() -> webWorkerTransform() -> sink`。行级进度优先使�?`progressBehavior()`，把 `run({ onEvent })` 留给诊断场景，因�?full observer 模式会发出内�?channel 事件�?
The browser CSV smoke fixture also supports larger local checks:

浏览�?CSV smoke fixture 也支持更大的本地检查：

```bash
npm run smoke:browser-csv-large
```

This is a large-ish correctness and boundary smoke, not a release SLA. It keeps `progressBehavior()` on the low-cost path and checks that CSV decoder channel metrics are not emitted.

这是偏大输入的正确性和边界 smoke，不是发�?SLA。它保持 `progressBehavior()` 的低成本路径，并检查不会发�?CSV decoder channel metrics�?
Worker:

Worker 线程�?
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
