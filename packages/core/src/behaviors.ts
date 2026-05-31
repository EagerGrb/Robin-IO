import {
  createDeadLetterRecord,
  RuntimeError,
  type DeadLetterRecord,
  type ErrorMode,
  type ProgressBehavior,
  type ProgressSnapshot,
  type RuntimeBehavior,
  type Sink
} from "./types.js"

export function progressBehavior(): ProgressBehavior {
  const snapshot = {
    recordsRead: 0,
    recordsHandled: 0,
    recordsSkipped: 0,
    recordsWritten: 0,
    batchesWritten: 0,
    errors: 0
  }

  return {
    name: "progress",
    onRecord(event) {
      if (event.action === "read") snapshot.recordsRead += event.count
      if (event.action === "handled") snapshot.recordsHandled += event.count
      if (event.action === "skipped") snapshot.recordsSkipped += event.count
      if (event.action === "written") snapshot.recordsWritten += event.count
    },
    onBatch(event) {
      snapshot.batchesWritten += 1
      snapshot.recordsWritten += event.batch.size
    },
    onError() {
      snapshot.errors += 1
    },
    getSnapshot(): ProgressSnapshot {
      return { ...snapshot }
    }
  }
}

export interface ErrorReporterBehavior extends RuntimeBehavior {
  getErrors(): readonly RuntimeError[]
  readonly mode: ErrorMode
}

export function errorReporterBehavior(mode: ErrorMode = "fail-fast"): ErrorReporterBehavior {
  const errors: RuntimeError[] = []

  return {
    name: "error-reporter",
    mode,
    onError(error) {
      errors.push(error)
    },
    getErrors() {
      return [...errors]
    }
  }
}

export interface DeadLetterBehavior extends RuntimeBehavior {
  getWrittenCount(): number
}

export function deadLetterBehavior(sink: Sink<DeadLetterRecord>, name = "dead-letter"): DeadLetterBehavior {
  let batchId = 0
  let written = 0

  return {
    name,
    async onStart(ctx) {
      await sink.open?.(ctx)
    },
    async onError(error, ctx) {
      const batch = {
        id: batchId++,
        items: [
          createDeadLetterRecord(error, {
            runtime: ctx.runtime,
            source: ctx.metadata.get("source") ?? undefined
          })
        ],
        size: 1,
        createdAt: Date.now()
      }
      await sink.write(batch, ctx)
      written += 1
    },
    async onFinish(ctx) {
      await sink.close?.(ctx)
    },
    getWrittenCount() {
      return written
    }
  }
}

export interface CancellationOptions {
  readonly timeoutMs?: number
  readonly maxErrors?: number
}

export function cancellationBehavior(options: CancellationOptions): RuntimeBehavior {
  let timer: ReturnType<typeof setTimeout> | undefined
  let errors = 0

  return {
    name: "cancellation",
    onStart(ctx) {
      if (options.timeoutMs !== undefined) {
        timer = setTimeout(
          () => ctx.abort(new RuntimeError(`Pipeline timed out after ${options.timeoutMs}ms`)),
          options.timeoutMs
        )
      }
    },
    onError(error, ctx) {
      errors += 1
      if (options.maxErrors !== undefined && errors >= options.maxErrors) {
        ctx.abort(new RuntimeError(`Pipeline aborted after ${errors} error(s)`, { cause: error }))
      }
    },
    onFinish() {
      if (timer) {
        clearTimeout(timer)
        timer = undefined
      }
    }
  }
}
