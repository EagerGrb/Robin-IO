import {
  CORE_RUNTIME_ERROR_CODES,
  RuntimeError,
  type Batch,
  type MaybePromise,
  type RuntimeContext,
  type Sink,
  type Source,
  type Transform
} from "@robbin-io/core"

export interface BlobSourceOptions {
  readonly name?: string
  readonly chunkSize?: number
}

export function blobSource(blob: Blob, options: BlobSourceOptions | string = {}): Source<Uint8Array> {
  const sourceOptions = normalizeBlobSourceOptions(options)
  const name = sourceOptions.name ?? "blob-source"

  return {
    kind: "source",
    name,
    async *read(ctx) {
      if (sourceOptions.chunkSize !== undefined) {
        yield* readBlobSlices(blob, sourceOptions.chunkSize, name, ctx)
        return
      }

      const reader = blob.stream().getReader()
      try {
        while (true) {
          if (ctx.signal.aborted) {
            await reader.cancel(ctx.signal.reason)
            return
          }
          const { done, value } = await reader.read()
          if (done) return
          await emitByteEvent(ctx, name, "read", value.byteLength)
          yield value
        }
      } finally {
        reader.releaseLock()
      }
    }
  }
}

export const fileSource = blobSource

export interface DownloadSinkOptions {
  readonly type?: string
  readonly name?: string
}

export interface DownloadSink extends Sink<Uint8Array | string> {
  getBlob(): Blob
  getObjectUrl(): string
  revokeObjectUrl(): void
  dispose(): void
  getBytesWritten(): number
}

export function downloadSink(options?: DownloadSinkOptions | string, name?: string): DownloadSink {
  const sinkOptions = normalizeDownloadSinkOptions(options, name)
  const type = sinkOptions.type ?? "application/octet-stream"
  const sinkName = sinkOptions.name ?? "download-sink"
  const chunks: Array<Uint8Array | string> = []
  let url: string | undefined
  let bytesWritten = 0

  const revokeObjectUrl = (): void => {
    if (url) {
      URL.revokeObjectURL(url)
      url = undefined
    }
  }

  return {
    kind: "sink",
    name: sinkName,
    async write(batch: Batch<Uint8Array | string>, ctx) {
      revokeObjectUrl()
      chunks.push(...batch.items)
      for (const item of batch.items) {
        const bytes = byteLength(item)
        bytesWritten += bytes
        await emitByteEvent(ctx, sinkName, "written", bytes)
      }
    },
    getBlob() {
      return new Blob(chunks.map(toBlobPart), { type })
    },
    getObjectUrl() {
      url ??= URL.createObjectURL(this.getBlob())
      return url
    },
    revokeObjectUrl,
    close() {
      revokeObjectUrl()
    },
    dispose() {
      revokeObjectUrl()
      chunks.length = 0
      bytesWritten = 0
    },
    getBytesWritten() {
      return bytesWritten
    }
  }
}

export interface WritableStreamSinkOptions {
  readonly name?: string
  readonly close?: boolean
  readonly abortOnError?: boolean
  readonly abortOnCancel?: boolean
}

export interface WritableStreamSink<T extends Uint8Array | string = Uint8Array> extends Sink<T> {
  getBytesWritten(): number
  getChunksWritten(): number
}

export function writableStreamSink<T extends Uint8Array | string = Uint8Array>(
  stream: WritableStream<T>,
  options: WritableStreamSinkOptions = {}
): WritableStreamSink<T> {
  const sinkName = options.name ?? "writable-stream-sink"
  const closeStream = options.close ?? true
  const abortOnError = options.abortOnError ?? true
  const abortOnCancel = options.abortOnCancel ?? true
  let writer: WritableStreamDefaultWriter<T> | undefined
  let bytesWritten = 0
  let chunksWritten = 0
  let finalized = false
  let abortCleanup: (() => void) | undefined

  const ensureWriter = (): WritableStreamDefaultWriter<T> => {
    if (writer === undefined) {
      writer = stream.getWriter()
    }
    return writer
  }

  const cleanupAbortListener = (): void => {
    abortCleanup?.()
    abortCleanup = undefined
  }

  return {
    kind: "sink",
    name: sinkName,
    open(ctx) {
      ensureWriter()
      if (ctx.signal.aborted) {
        void abortWritableStreamSink(ctx.signal.reason)
        return
      }

      const abort = (): void => {
        void abortWritableStreamSink(ctx.signal.reason)
      }
      ctx.signal.addEventListener("abort", abort, { once: true })
      abortCleanup = () => ctx.signal.removeEventListener("abort", abort)
    },
    async write(batch: Batch<T>, ctx) {
      const activeWriter = ensureWriter()
      for (const item of batch.items) {
        assertWritableSinkNotAborted(ctx)
        await activeWriter.write(item)
        const bytes = byteLength(item)
        bytesWritten += bytes
        chunksWritten += 1
        await emitByteEvent(ctx, sinkName, "written", bytes)
      }
    },
    async close(ctx) {
      cleanupAbortListener()
      if (ctx.signal.aborted && abortOnCancel) {
        await abortWritableStreamSink(ctx.signal.reason)
        return
      }
      if (ctx.metadata.get("pipeline.failed") === true && abortOnError) {
        await abortWritableStreamSink(
          new RuntimeError("Writable stream sink aborted because the pipeline failed", {
            code: "WRITABLE_STREAM_ABORTED",
            stage: sinkName
          })
        )
        return
      }
      await closeWritableStreamSink()
    },
    getBytesWritten() {
      return bytesWritten
    },
    getChunksWritten() {
      return chunksWritten
    }
  }

  async function closeWritableStreamSink(): Promise<void> {
    if (finalized) {
      return
    }
    finalized = true
    const activeWriter = writer
    writer = undefined
    if (activeWriter === undefined) {
      return
    }
    try {
      if (closeStream) {
        await activeWriter.close()
      }
    } finally {
      activeWriter.releaseLock()
    }
  }

  async function abortWritableStreamSink(reason: unknown): Promise<void> {
    if (finalized) {
      return
    }
    finalized = true
    const activeWriter = writer
    writer = undefined
    if (activeWriter === undefined) {
      return
    }
    try {
      await activeWriter.abort(reason)
    } finally {
      activeWriter.releaseLock()
    }
  }
}

function normalizeBlobSourceOptions(options: BlobSourceOptions | string): BlobSourceOptions {
  return typeof options === "string" ? { name: options } : options
}

function normalizeDownloadSinkOptions(options: DownloadSinkOptions | string | undefined, name: string | undefined) {
  return typeof options === "string" || options === undefined ? { type: options, name } : options
}

async function* readBlobSlices(
  blob: Blob,
  chunkSize: number,
  name: string,
  ctx: Parameters<Source<Uint8Array>["read"]>[0]
): AsyncIterable<Uint8Array> {
  const size = normalizeChunkSize(chunkSize)

  for (let offset = 0; offset < blob.size; offset += size) {
    if (ctx.signal.aborted) {
      return
    }
    const chunk = new Uint8Array(await blob.slice(offset, offset + size).arrayBuffer())
    await emitByteEvent(ctx, name, "read", chunk.byteLength)
    yield chunk
  }
}

function normalizeChunkSize(chunkSize: number): number {
  return Number.isInteger(chunkSize) && chunkSize > 0 ? chunkSize : 64 * 1024
}

function toBlobPart(chunk: Uint8Array | string): BlobPart {
  if (typeof chunk === "string") {
    return chunk
  }

  return chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength) as ArrayBuffer
}

function byteLength(item: Uint8Array | string): number {
  return typeof item === "string" ? new TextEncoder().encode(item).byteLength : item.byteLength
}

function assertWritableSinkNotAborted(ctx: RuntimeContext): void {
  if (ctx.signal.aborted) {
    throw new RuntimeError("Pipeline aborted", {
      code: CORE_RUNTIME_ERROR_CODES.pipelineAborted,
      cause: ctx.signal.reason
    })
  }
}

async function emitByteEvent(
  ctx: Parameters<Sink<Uint8Array | string>["write"]>[1],
  stage: string,
  action: "read" | "written",
  bytes: number
): Promise<void> {
  await ctx.emit({
    type: "record",
    stage,
    action,
    count: 0,
    timestamp: Date.now(),
    metadata: { bytes }
  })
}

export interface WebWorkerTransformOptions<I> {
  readonly name?: string
  readonly concurrency?: number
  readonly maxPending?: number
  readonly timeoutMs?: number
  readonly transfer?: (input: I) => readonly Transferable[]
}

export interface WorkerTransformRequest<I> {
  readonly type: "io:transform"
  readonly id: number
  readonly input: I
}

export interface WorkerTransformCancel {
  readonly type: "io:cancel"
  readonly id: number
  readonly reason?: unknown
}

export interface WorkerTransformContext {
  readonly signal: AbortSignal
}

export interface WorkerTransformSuccess<O> {
  readonly type: "io:transform-result"
  readonly id: number
  readonly ok: true
  readonly output?: O | O[]
}

export interface WorkerTransformFailure {
  readonly type: "io:transform-result"
  readonly id: number
  readonly ok: false
  readonly error: {
    readonly code?: string
    readonly message: string
    readonly metadata?: Record<string, unknown>
  }
}

export type WorkerTransformMessage<I> = WorkerTransformRequest<I> | WorkerTransformCancel
export type WorkerTransformResponse<O> = WorkerTransformSuccess<O> | WorkerTransformFailure

export function webWorkerTransform<I, O>(
  workerFactory: () => Worker,
  options: WebWorkerTransformOptions<I> = {}
): Transform<I, O> {
  let worker: Worker | undefined
  let nextId = 0
  let activeRequests = 0
  const pendingLimit = options.maxPending ?? options.concurrency
  const pendingWaiters: Array<{
    readonly resolve: () => void
    readonly reject: (error: RuntimeError) => void
  }> = []
  const pending = new Map<
    number,
    {
      resolve: (value: O | O[] | undefined) => void
      reject: (error: unknown) => void
      timer?: ReturnType<typeof setTimeout>
    }
  >()

  const ensureWorker = (): Worker => {
    if (worker) {
      return worker
    }

    worker = workerFactory()
    worker.addEventListener("message", (event: MessageEvent<WorkerTransformResponse<O>>) => {
      const message = event.data
      if (!message || message.type !== "io:transform-result") {
        return
      }

      const request = pending.get(message.id)
      if (!request) {
        return
      }

      pending.delete(message.id)
      releasePendingSlot()
      if (request.timer) {
        clearTimeout(request.timer)
      }

      if (message.ok) {
        request.resolve(message.output)
        return
      }

      request.reject(
        new RuntimeError(message.error.message, {
          code: message.error.code ?? "WORKER_TRANSFORM_ERROR",
          stage: options.name ?? "web-worker-transform",
          metadata: message.error.metadata
        })
      )
    })

    worker.addEventListener("error", (event) => {
      rejectAll(
        new RuntimeError(event.message, {
          code: "WORKER_RUNTIME_ERROR",
          stage: options.name ?? "web-worker-transform"
        })
      )
      worker?.terminate()
      worker = undefined
    })

    return worker
  }

  const rejectAll = (error: RuntimeError): void => {
    for (const request of pending.values()) {
      if (request.timer) {
        clearTimeout(request.timer)
      }
      request.reject(error)
    }
    pending.clear()
    activeRequests = 0
    for (const waiter of pendingWaiters.splice(0)) {
      waiter.reject(error)
    }
  }

  const acquirePendingSlot = async (): Promise<void> => {
    if (pendingLimit === undefined || activeRequests < Math.max(1, pendingLimit)) {
      activeRequests += 1
      return
    }

    await new Promise<void>((resolve, reject) => {
      pendingWaiters.push({ resolve, reject })
    })
    activeRequests += 1
  }

  const releasePendingSlot = (): void => {
    activeRequests = Math.max(0, activeRequests - 1)
    const waiter = pendingWaiters.shift()
    waiter?.resolve()
  }

  return {
    kind: "transform",
    name: options.name ?? "web-worker-transform",
    concurrency: options.concurrency,
    open() {
      ensureWorker()
    },
    async handle(input) {
      const activeWorker = ensureWorker()
      await acquirePendingSlot()
      const id = nextId++

      return new Promise<O | O[] | undefined>((resolve, reject) => {
        const timer =
          options.timeoutMs === undefined
            ? undefined
            : setTimeout(() => {
                pending.delete(id)
                releasePendingSlot()
                postCancel(activeWorker, id, "timeout")
                reject(
                  new RuntimeError(`Worker transform timed out after ${options.timeoutMs}ms`, {
                    code: "WORKER_TRANSFORM_TIMEOUT",
                    stage: options.name ?? "web-worker-transform"
                  })
                )
              }, options.timeoutMs)

        pending.set(id, {
          resolve(value) {
            resolve(value)
          },
          reject(error) {
            reject(error)
          },
          timer
        })
        try {
          activeWorker.postMessage({ type: "io:transform", id, input } satisfies WorkerTransformRequest<I>, [
            ...(options.transfer?.(input) ?? [])
          ])
        } catch (error) {
          pending.delete(id)
          releasePendingSlot()
          if (timer) {
            clearTimeout(timer)
          }
          reject(error)
        }
      })
    },
    close() {
      if (worker) {
        for (const id of pending.keys()) {
          postCancel(worker, id, "close")
        }
      }
      rejectAll(
        new RuntimeError("Worker transform closed", {
          code: "WORKER_TRANSFORM_CLOSED",
          stage: options.name ?? "web-worker-transform"
        })
      )
      worker?.terminate()
      worker = undefined
    }
  }
}

function postCancel(worker: Worker, id: number, reason?: unknown): void {
  try {
    worker.postMessage({ type: "io:cancel", id, reason } satisfies WorkerTransformCancel)
  } catch {
    // The worker may already be terminating; cancellation is best-effort.
  }
}

export function exposeWorkerTransform<I, O>(
  handler: (input: I, ctx: WorkerTransformContext) => MaybePromise<O | O[] | undefined>
): void {
  const controllers = new Map<number, AbortController>()

  globalThis.addEventListener("message", async (event: MessageEvent<WorkerTransformMessage<I>>) => {
    const message = event.data
    if (!message) {
      return
    }

    if (message.type === "io:cancel") {
      const controller = controllers.get(message.id)
      if (controller && !controller.signal.aborted) {
        controller.abort(message.reason)
      }
      return
    }

    if (message.type !== "io:transform") {
      return
    }

    const controller = new AbortController()
    controllers.set(message.id, controller)
    try {
      const output = await handler(message.input, { signal: controller.signal })
      controllers.delete(message.id)
      globalThis.postMessage({
        type: "io:transform-result",
        id: message.id,
        ok: true,
        output
      } satisfies WorkerTransformSuccess<O>)
    } catch (error) {
      controllers.delete(message.id)
      globalThis.postMessage({
        type: "io:transform-result",
        id: message.id,
        ok: false,
        error: {
          code: error instanceof RuntimeError ? error.code : "WORKER_TRANSFORM_ERROR",
          message: error instanceof Error ? error.message : String(error),
          metadata: error instanceof RuntimeError ? error.metadata : undefined
        }
      } satisfies WorkerTransformFailure)
    }
  })
}
