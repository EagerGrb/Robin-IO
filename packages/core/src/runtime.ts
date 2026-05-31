import { applyBatchMetrics, applyMetricEvent, applyRecordMetrics, InMemoryMetricsCollector } from "./metrics.js"
import {
  CORE_RUNTIME_ERROR_CODES,
  RuntimeError,
  type Logger,
  type MaybePromise,
  type RunOptions,
  type RuntimeBehavior,
  type RuntimeContext,
  type RuntimeEvent
} from "./types.js"

const noopLogger: Logger = {}

export interface RuntimeState {
  readonly ctx: RuntimeContext
  readonly controller: AbortController
  readonly behaviors: readonly RuntimeBehavior[]
  readonly recordBehaviors: readonly RuntimeBehavior[]
  readonly batchBehaviors: readonly RuntimeBehavior[]
  readonly errors: RuntimeError[]
  readonly onEvent?: RunOptions["onEvent"]
  readonly detailedEvents: boolean
  readonly detailedChannelEvents: boolean
}

export function createRuntimeState(options: RunOptions = {}): RuntimeState {
  const controller = new AbortController()
  const metrics = options.metrics ?? new InMemoryMetricsCollector()
  const errors: RuntimeError[] = []
  const behaviors = options.behaviors ?? []
  const eventBehaviors = behaviors.filter((behavior) => behavior.onEvent !== undefined)
  const recordBehaviors = behaviors.filter((behavior) => behavior.onRecord !== undefined)
  const batchBehaviors = behaviors.filter((behavior) => behavior.onBatch !== undefined)
  const hasEventObservers = options.onEvent !== undefined || eventBehaviors.length > 0

  if (options.signal) {
    if (options.signal.aborted) {
      controller.abort(options.signal.reason)
    } else {
      options.signal.addEventListener("abort", () => controller.abort(options.signal?.reason), { once: true })
    }
  }

  const ctx: RuntimeContext = {
    signal: controller.signal,
    metrics,
    logger: options.logger ?? noopLogger,
    runtime: options.runtime ?? "unknown",
    metadata: new Map(),
    detailedChannelEvents: hasEventObservers,
    abort(reason?: unknown): void {
      if (!controller.signal.aborted) {
        controller.abort(reason)
      }
    },
    emit(event: RuntimeEvent): void | Promise<void> {
      applyMetricEvent(metrics, event)
      if (event.type === "error") {
        errors.push(event.error)
      }
      if (!hasEventObservers) {
        return
      }
      return notifyEventObservers(event, ctx, options.onEvent, eventBehaviors)
    }
  }

  return {
    ctx,
    controller,
    behaviors,
    recordBehaviors,
    batchBehaviors,
    errors,
    onEvent: options.onEvent,
    detailedEvents: hasEventObservers,
    detailedChannelEvents: hasEventObservers
  }
}

export function assertNotAborted(ctx: RuntimeContext): void {
  if (ctx.signal.aborted) {
    throw new RuntimeError("Pipeline aborted", {
      code: CORE_RUNTIME_ERROR_CODES.pipelineAborted,
      cause: ctx.signal.reason
    })
  }
}

export function toRuntimeError(error: unknown, stage?: string, input?: unknown): RuntimeError {
  if (error instanceof RuntimeError) {
    return error
  }

  const message = error instanceof Error ? error.message : String(error)
  return new RuntimeError(message, { cause: error, stage, input })
}

export async function notifyStart(state: RuntimeState): Promise<void> {
  const event = { type: "start" as const, timestamp: Date.now() }
  await notifyEvent(state, event)
  for (const behavior of state.behaviors) {
    await behavior.onStart?.(state.ctx)
  }
}

export async function notifyFinish(state: RuntimeState): Promise<void> {
  const event = { type: "finish" as const, timestamp: Date.now() }
  await notifyEvent(state, event)
  for (const behavior of state.behaviors) {
    await behavior.onFinish?.(state.ctx)
  }
}

export async function notifyError(state: RuntimeState, error: RuntimeError): Promise<void> {
  await notifyEvent(state, { type: "error", error, timestamp: Date.now(), metadata: error.metadata })
  for (const behavior of state.behaviors) {
    await behavior.onError?.(error, state.ctx)
  }
}

export function notifyRecord(
  state: RuntimeState,
  stage: string,
  action: "read" | "handled" | "skipped" | "written",
  count = 1,
  metadata?: import("./types.js").RuntimeMetadata
): Promise<void> | undefined {
  if (!state.detailedEvents && state.recordBehaviors.length === 0) {
    applyRecordMetrics(state.ctx.metrics, stage, action, count)
    return undefined
  }

  const event = { type: "record" as const, stage, action, count, timestamp: Date.now(), metadata }
  const notified = notifyEventFast(state, event)
  if (state.recordBehaviors.length === 0) {
    return notified
  }
  return notifyRecordBehaviors(state, event, notified)
}

async function notifyRecordBehaviors(
  state: RuntimeState,
  event: import("./types.js").RecordEvent,
  notified: Promise<void> | undefined
): Promise<void> {
  if (notified) {
    await notified
  }
  for (const behavior of state.recordBehaviors) {
    await behavior.onRecord?.(event, state.ctx)
  }
}

export function notifyBatch(
  state: RuntimeState,
  batch: import("./types.js").Batch<unknown>,
  stage: string,
  metadata?: import("./types.js").RuntimeMetadata
): Promise<void> | undefined {
  if (!state.detailedEvents && state.batchBehaviors.length === 0) {
    applyBatchMetrics(state.ctx.metrics, batch)
    return undefined
  }

  const event = { type: "batch" as const, stage, batch, timestamp: Date.now(), metadata }
  const notified = notifyEventFast(state, event)
  if (state.batchBehaviors.length === 0) {
    return notified
  }
  return notifyBatchBehaviors(state, event, notified)
}

async function notifyBatchBehaviors(
  state: RuntimeState,
  event: import("./types.js").BatchEvent,
  notified: Promise<void> | undefined
): Promise<void> {
  if (notified) {
    await notified
  }
  for (const behavior of state.batchBehaviors) {
    await behavior.onBatch?.(event, state.ctx)
  }
}

async function notifyEvent(state: RuntimeState, event: RuntimeEvent): Promise<void> {
  const notified = notifyEventFast(state, event)
  if (notified) await notified
}

function notifyEventFast(state: RuntimeState, event: RuntimeEvent): Promise<void> | undefined {
  return emitRuntimeEvent(state.ctx, event)
}

export function emitRuntimeEvent(ctx: RuntimeContext, event: RuntimeEvent): Promise<void> | undefined {
  const emitted = ctx.emit(event)
  return isPromiseLike(emitted) ? Promise.resolve(emitted) : undefined
}

function isPromiseLike<T>(value: MaybePromise<T>): value is Promise<T> {
  return value !== undefined && value !== null && typeof (value as { then?: unknown }).then === "function"
}

async function notifyEventObservers(
  event: RuntimeEvent,
  ctx: RuntimeContext,
  onEvent: RunOptions["onEvent"],
  behaviors: readonly RuntimeBehavior[]
): Promise<void> {
  await onEvent?.(event, ctx)
  for (const behavior of behaviors) {
    await behavior.onEvent?.(event, ctx)
  }
}
