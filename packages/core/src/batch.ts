import { assertNotAborted } from "./runtime.js"
import { CORE_RUNTIME_ERROR_CODES, RuntimeError, type Batch, type BatchOptions, type RuntimeContext } from "./types.js"

const defaultBatchSize = 1

export async function* batchIterable<T>(
  input: AsyncIterable<T>,
  options: BatchOptions<T> | undefined,
  ctx: RuntimeContext
): AsyncIterable<Batch<T>> {
  const maxSize = Math.max(1, options?.size ?? defaultBatchSize)
  const maxWaitMs = options?.maxWaitMs
  const maxBytes = options?.maxBytes
  const estimateBytes = options?.estimateBytes
  let current: T[] = []
  let currentBytes = 0
  let id = 0

  const flush = (): Batch<T> | undefined => {
    if (current.length === 0) {
      return undefined
    }

    const batch: Batch<T> = {
      id: id++,
      items: current,
      size: current.length,
      createdAt: Date.now(),
      bytes: estimateBytes ? currentBytes : undefined
    }
    current = []
    currentBytes = 0
    return batch
  }

  const iterator = input[Symbol.asyncIterator]()
  let nextItem = iterator.next()
  let waitTimer: Promise<"timeout"> | undefined

  while (true) {
    assertNotAborted(ctx)
    waitTimer ??=
      current.length > 0 && maxWaitMs !== undefined
        ? delay(maxWaitMs, ctx.signal).then(() => "timeout" as const)
        : undefined

    const result = await Promise.race(waitTimer ? [nextItem, waitTimer] : [nextItem])

    if (result === "timeout") {
      waitTimer = undefined
      const batch = flush()
      if (batch) {
        yield batch
      }
      continue
    }

    if (result.done) {
      break
    }

    waitTimer = undefined
    const item = result.value
    const bytes = estimateBytes?.(item) ?? 0

    if (maxBytes !== undefined && current.length > 0 && currentBytes + bytes > maxBytes) {
      const batch = flush()
      if (batch) {
        yield batch
      }
    }

    current.push(item)
    currentBytes += bytes

    if (current.length >= maxSize) {
      const batch = flush()
      if (batch) {
        yield batch
      }
    }

    nextItem = iterator.next()
  }

  const batch = flush()
  if (batch) {
    yield batch
  }
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(abortError(signal))
      return
    }

    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    const onAbort = (): void => {
      clearTimeout(timer)
      signal.removeEventListener("abort", onAbort)
      reject(abortError(signal))
    }
    signal.addEventListener("abort", onAbort, { once: true })
  })
}

function abortError(signal: AbortSignal): RuntimeError {
  return new RuntimeError("Pipeline aborted", { code: CORE_RUNTIME_ERROR_CODES.pipelineAborted, cause: signal.reason })
}
