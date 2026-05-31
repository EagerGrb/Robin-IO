import { describe, expect, it, vi } from "vitest"
import { map, pipeline, type RuntimeEvent } from "@robbin-io/core"
import {
  blobSource,
  downloadSink,
  webWorkerTransform,
  writableStreamSink,
  type WorkerTransformCancel,
  type WorkerTransformMessage,
  type WorkerTransformRequest,
  type WorkerTransformResponse
} from "./index.js"

describe("blobSource", () => {
  it("reads blobs with an explicit chunk size and emits byte metadata", async () => {
    const chunks: Uint8Array[] = []
    const byteEvents: Array<{ stage: string; action: string; bytes: unknown }> = []

    const result = await pipeline()
      .from(blobSource(new Blob(["hello"]), { name: "file-chunks", chunkSize: 2 }))
      .to({
        kind: "sink",
        name: "collector",
        write(batch) {
          chunks.push(...batch.items)
        }
      })
      .run({
        runtime: "browser",
        onEvent(event: RuntimeEvent) {
          if (event.type === "record" && typeof event.metadata?.bytes === "number") {
            byteEvents.push({ stage: event.stage, action: event.action, bytes: event.metadata.bytes })
          }
        }
      })

    expect(result.ok).toBe(true)
    expect(chunks.map((chunk) => new TextDecoder().decode(chunk))).toEqual(["he", "ll", "o"])
    expect(byteEvents).toEqual([
      { stage: "file-chunks", action: "read", bytes: 2 },
      { stage: "file-chunks", action: "read", bytes: 2 },
      { stage: "file-chunks", action: "read", bytes: 1 }
    ])
  })
})

describe("downloadSink", () => {
  it("records bytes, creates object URLs, and revokes them on close or dispose", async () => {
    const createObjectUrl = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:test")
    const revokeObjectUrl = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined)
    const byteEvents: Array<{ stage: string; action: string; bytes: unknown }> = []
    const sink = downloadSink({ type: "text/plain", name: "download" })

    const result = await pipeline()
      .from({
        kind: "source",
        name: "chunks",
        async *read() {
          yield "hello"
          yield new TextEncoder().encode("!")
        }
      })
      .to(sink)
      .run({
        runtime: "browser",
        onEvent(event) {
          if (event.type === "record" && typeof event.metadata?.bytes === "number") {
            byteEvents.push({ stage: event.stage, action: event.action, bytes: event.metadata.bytes })
          }
        }
      })

    expect(result.ok).toBe(true)
    expect(sink.getBytesWritten()).toBe(6)
    expect(await sink.getBlob().text()).toBe("hello!")
    expect(sink.getObjectUrl()).toBe("blob:test")
    expect(createObjectUrl).toHaveBeenCalledTimes(1)

    sink.close?.({} as never)
    expect(revokeObjectUrl).toHaveBeenCalledWith("blob:test")

    expect(byteEvents).toEqual([
      { stage: "download", action: "written", bytes: 5 },
      { stage: "download", action: "written", bytes: 1 }
    ])

    sink.dispose()
    expect(sink.getBytesWritten()).toBe(0)
    expect(await sink.getBlob().text()).toBe("")

    createObjectUrl.mockRestore()
    revokeObjectUrl.mockRestore()
  })
})

describe("writableStreamSink", () => {
  it("writes chunks to a WritableStream, closes it, and records byte metadata", async () => {
    const chunks: Array<Uint8Array | string> = []
    const byteEvents: Array<{ stage: string; action: string; bytes: unknown }> = []
    let closed = false

    const stream = new WritableStream<Uint8Array | string>({
      write(chunk) {
        chunks.push(chunk)
      },
      close() {
        closed = true
      }
    })
    const sink = writableStreamSink<Uint8Array | string>(stream, { name: "stream-export" })

    const result = await pipeline()
      .from({
        kind: "source",
        name: "chunks",
        async *read() {
          yield "hello"
          yield new TextEncoder().encode("!")
        }
      })
      .to(sink)
      .run({
        runtime: "browser",
        onEvent(event) {
          if (event.type === "record" && typeof event.metadata?.bytes === "number") {
            byteEvents.push({ stage: event.stage, action: event.action, bytes: event.metadata.bytes })
          }
        }
      })

    expect(result.ok).toBe(true)
    expect(closed).toBe(true)
    expect(sink.getBytesWritten()).toBe(6)
    expect(sink.getChunksWritten()).toBe(2)
    expect(chunks[0]).toBe("hello")
    expect(new TextDecoder().decode(chunks[1] as Uint8Array)).toBe("!")
    expect(byteEvents).toEqual([
      { stage: "stream-export", action: "written", bytes: 5 },
      { stage: "stream-export", action: "written", bytes: 1 }
    ])
  })

  it("aborts the WritableStream when the pipeline fails", async () => {
    let closed = false
    let abortReason: unknown
    const stream = new WritableStream<string>({
      close() {
        closed = true
      },
      abort(reason) {
        abortReason = reason
      }
    })
    const sink = writableStreamSink<string>(stream, { name: "stream-export" })

    const result = await pipeline()
      .from({
        kind: "source",
        name: "records",
        async *read() {
          yield "first"
        }
      })
      .through(
        map((item: string) => {
          throw new Error(`failed ${item}`)
        })
      )
      .to(sink)
      .run({ runtime: "browser" })

    expect(result.ok).toBe(false)
    expect(closed).toBe(false)
    expect(abortReason).toMatchObject({ code: "WRITABLE_STREAM_ABORTED", stage: "stream-export" })
  })
})

describe("webWorkerTransform", () => {
  it("waits when maxPending is reached before posting another worker request", async () => {
    const worker = new TestWorker<number, number>()
    const transform = webWorkerTransform<number, number>(() => worker as unknown as Worker, {
      maxPending: 1,
      concurrency: 3
    })

    const first = transform.handle(1, {} as never) as Promise<number>
    const second = transform.handle(2, {} as never) as Promise<number>

    await Promise.resolve()
    expect(worker.requests.map((request) => request.input)).toEqual([1])

    worker.resolve(0, 10)
    await expect(first).resolves.toBe(10)
    await waitFor(() => worker.requests.length === 2)
    expect(worker.requests.map((request) => request.input)).toEqual([1, 2])

    worker.resolve(1, 20)
    await expect(second).resolves.toBe(20)
  })

  it("rejects pending and waiting requests when closed", async () => {
    const worker = new TestWorker<number, number>()
    const transform = webWorkerTransform<number, number>(() => worker as unknown as Worker, {
      maxPending: 1,
      concurrency: 2,
      name: "worker-map"
    })

    const first = transform.handle(1, {} as never) as Promise<number>
    const second = transform.handle(2, {} as never) as Promise<number>

    await Promise.resolve()
    transform.close?.({} as never)

    await expect(first).rejects.toMatchObject({ code: "WORKER_TRANSFORM_CLOSED", stage: "worker-map" })
    await expect(second).rejects.toMatchObject({ code: "WORKER_TRANSFORM_CLOSED", stage: "worker-map" })
    expect(worker.cancellations).toEqual([{ type: "io:cancel", id: 0, reason: "close" }])
    expect(worker.terminated).toBe(true)
  })

  it("sends cancel when a worker request times out", async () => {
    const worker = new TestWorker<number, number>()
    const transform = webWorkerTransform<number, number>(() => worker as unknown as Worker, {
      timeoutMs: 1
    })

    const result = transform.handle(1, {} as never) as Promise<number>
    await expect(result).rejects.toMatchObject({ code: "WORKER_TRANSFORM_TIMEOUT" })
    expect(worker.cancellations).toEqual([{ type: "io:cancel", id: 0, reason: "timeout" }])
  })

  it("passes transferables to worker.postMessage", async () => {
    const worker = new TestWorker<Uint8Array, number>()
    const transform = webWorkerTransform<Uint8Array, number>(() => worker as unknown as Worker, {
      transfer: (input) => [input.buffer]
    })
    const input = new Uint8Array([1, 2, 3])

    const result = transform.handle(input, {} as never) as Promise<number>
    await Promise.resolve()

    expect(worker.transfers[0]).toEqual([input.buffer])
    worker.resolve(0, 3)
    await expect(result).resolves.toBe(3)
  })
})

class TestWorker<I, O> {
  readonly requests: Array<WorkerTransformRequest<I>> = []
  readonly cancellations: WorkerTransformCancel[] = []
  readonly transfers: Transferable[][] = []
  private readonly messageListeners: Array<(event: MessageEvent<WorkerTransformResponse<O>>) => void> = []
  private readonly errorListeners: Array<(event: ErrorEvent) => void> = []
  terminated = false

  addEventListener(type: "message", listener: (event: MessageEvent<WorkerTransformResponse<O>>) => void): void
  addEventListener(type: "error", listener: (event: ErrorEvent) => void): void
  addEventListener(type: "message" | "error", listener: (event: never) => void): void {
    if (type === "message") {
      this.messageListeners.push(listener as (event: MessageEvent<WorkerTransformResponse<O>>) => void)
      return
    }
    this.errorListeners.push(listener as (event: ErrorEvent) => void)
  }

  postMessage(message: WorkerTransformMessage<I>, transfer: Transferable[] = []): void {
    if (message.type === "io:cancel") {
      this.cancellations.push(message)
      return
    }

    this.transfers.push(transfer)
    this.requests.push(message)
  }

  terminate(): void {
    this.terminated = true
  }

  resolve(id: number, output: O | O[] | undefined): void {
    this.dispatchMessage({
      type: "io:transform-result",
      id,
      ok: true,
      output
    })
  }

  private dispatchMessage(message: WorkerTransformResponse<O>): void {
    const event = { data: message } as MessageEvent<WorkerTransformResponse<O>>
    for (const listener of this.messageListeners) {
      listener(event)
    }
  }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    if (predicate()) {
      return
    }
    await Promise.resolve()
  }
  throw new Error("Timed out waiting for condition")
}
