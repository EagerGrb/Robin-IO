export type MaybePromise<T> = T | Promise<T>

export type RuntimeMetadata = Readonly<Record<string, unknown>>
export type RuntimeErrorCode = string
export type DeadLetterPayload = Readonly<Record<string, unknown>>

export const CORE_RUNTIME_ERROR_CODES = {
  pipelineAborted: "PIPELINE_ABORTED",
  pipelineAlreadyRun: "PIPELINE_ALREADY_RUN",
  invalidPipelineStatus: "INVALID_PIPELINE_STATUS",
  channelClosed: "CHANNEL_CLOSED",
  sourceReadError: "SOURCE_READ_ERROR",
  sinkWriteError: "SINK_WRITE_ERROR",
  codecDecodeError: "CODEC_DECODE_ERROR",
  codecEncodeError: "CODEC_ENCODE_ERROR",
  validationError: "VALIDATION_ERROR",
  batchFlushError: "BATCH_FLUSH_ERROR",
  resourceOpenError: "RESOURCE_OPEN_ERROR",
  resourceCloseError: "RESOURCE_CLOSE_ERROR"
} as const

export type CoreRuntimeErrorCode = (typeof CORE_RUNTIME_ERROR_CODES)[keyof typeof CORE_RUNTIME_ERROR_CODES]

export interface Batch<T> {
  readonly id: number
  readonly items: readonly T[]
  readonly size: number
  readonly createdAt: number
  readonly bytes?: number
}

export interface BatchOptions<T = unknown> {
  readonly size?: number
  readonly maxWaitMs?: number
  readonly maxBytes?: number
  readonly estimateBytes?: (item: T) => number
}

export interface Source<T> {
  readonly kind: "source"
  readonly name: string
  open?(ctx: RuntimeContext): MaybePromise<void>
  read(ctx: RuntimeContext): AsyncIterable<T>
  close?(ctx: RuntimeContext): MaybePromise<void>
}

export interface Sink<T> {
  readonly kind: "sink"
  readonly name: string
  open?(ctx: RuntimeContext): MaybePromise<void>
  write(batch: Batch<T>, ctx: RuntimeContext): MaybePromise<void>
  close?(ctx: RuntimeContext): MaybePromise<void>
}

export interface StageOptions {
  readonly name?: string
  readonly concurrency?: number
  readonly preserveOrder?: boolean
  readonly maxInFlight?: number
}

export interface Transform<I, O> {
  readonly kind: "transform"
  readonly name: string
  readonly concurrency?: number
  readonly preserveOrder?: boolean
  readonly maxInFlight?: number
  open?(ctx: RuntimeContext): MaybePromise<void>
  handle(input: I, ctx: RuntimeContext): MaybePromise<O | O[] | undefined>
  close?(ctx: RuntimeContext): MaybePromise<void>
}

export interface Decoder<T> {
  readonly kind: "decoder"
  readonly name: string
  open?(ctx: RuntimeContext): MaybePromise<void>
  decode(input: AsyncIterable<Uint8Array>, ctx: RuntimeContext): AsyncIterable<T>
  close?(ctx: RuntimeContext): MaybePromise<void>
}

export interface Encoder<T> {
  readonly kind: "encoder"
  readonly name: string
  open?(ctx: RuntimeContext): MaybePromise<void>
  encode(input: AsyncIterable<T>, ctx: RuntimeContext): AsyncIterable<Uint8Array>
  close?(ctx: RuntimeContext): MaybePromise<void>
}

export type PipelineStage<I = unknown, O = unknown> = Transform<I, O> | Decoder<O> | Encoder<I>

export interface Logger {
  debug?(message: string, meta?: unknown): void
  info?(message: string, meta?: unknown): void
  warn?(message: string, meta?: unknown): void
  error?(message: string, meta?: unknown): void
}

export interface MetricsCollector {
  increment(name: string, value?: number): void
  observe(name: string, value: number): void
  snapshot(): Record<string, number>
}

export type RuntimeKind = "browser" | "node" | "unknown"

export interface RuntimeContext {
  readonly signal: AbortSignal
  readonly metrics: MetricsCollector
  readonly logger: Logger
  readonly runtime: RuntimeKind
  readonly metadata: Map<string, unknown>
  readonly detailedChannelEvents?: boolean
  abort(reason?: unknown): void
  emit(event: RuntimeEvent): MaybePromise<void>
}

export type RuntimeEvent =
  | RecordEvent
  | BatchEvent
  | StageEvent
  | StageItemEvent
  | StageWaitEvent
  | ChannelEvent
  | SinkWriteEvent
  | BatchFlushEvent
  | ErrorEvent
  | LifecycleEvent

export interface RuntimeEventBase {
  readonly timestamp: number
  readonly metadata?: RuntimeMetadata
}

export interface InternalRuntimeEventBase extends RuntimeEventBase {}

export interface RecordEvent extends RuntimeEventBase {
  readonly type: "record"
  readonly stage: string
  readonly action: "read" | "handled" | "skipped" | "written"
  readonly count: number
}

export interface BatchEvent extends RuntimeEventBase {
  readonly type: "batch"
  readonly stage: string
  readonly batch: Batch<unknown>
}

export type ChannelOperation = "push" | "pull"

export interface StageEvent extends RuntimeEventBase {
  readonly type: "stage.start" | "stage.end"
  readonly stage: string
  readonly name?: string
  readonly kind?: string
  readonly durationMs?: number
}

export interface StageItemEvent extends RuntimeEventBase {
  readonly type: "stage.item.start" | "stage.item.end"
  readonly stage: string
  readonly name?: string
  readonly kind?: string
  readonly input?: unknown
  readonly durationMs?: number
}

export interface StageWaitEvent extends RuntimeEventBase {
  readonly type: "stage.wait"
  readonly stage: string
  readonly name?: string
  readonly kind?: string
  readonly operation?: string
  readonly durationMs: number
}

export interface ChannelEvent extends RuntimeEventBase {
  readonly type: "channel.push" | "channel.pull" | "channel.wait" | "channel.backpressure"
  readonly channel: string
  readonly size: number
  readonly capacity: number
  readonly operation?: ChannelOperation
  readonly durationMs?: number
}

export interface SinkWriteEvent extends RuntimeEventBase {
  readonly type: "sink.write.start" | "sink.write.end"
  readonly stage: string
  readonly batch: Batch<unknown>
  readonly durationMs?: number
}

export interface BatchFlushEvent extends RuntimeEventBase {
  readonly type: "batch.flush.start" | "batch.flush.end"
  readonly stage: string
  readonly batch?: Batch<unknown>
  readonly durationMs?: number
}

export interface ErrorEvent extends RuntimeEventBase {
  readonly type: "error"
  readonly error: RuntimeError
}

export interface LifecycleEvent extends RuntimeEventBase {
  readonly type: "start" | "finish"
}

export interface RuntimeBehavior {
  readonly name: string
  onEvent?(event: RuntimeEvent, ctx: RuntimeContext): MaybePromise<void>
  onStart?(ctx: RuntimeContext): MaybePromise<void>
  onRecord?(event: RecordEvent, ctx: RuntimeContext): MaybePromise<void>
  onBatch?(event: BatchEvent, ctx: RuntimeContext): MaybePromise<void>
  onError?(error: RuntimeError, ctx: RuntimeContext): MaybePromise<void>
  onFinish?(ctx: RuntimeContext): MaybePromise<void>
}

export interface ProgressSnapshot {
  readonly recordsRead: number
  readonly recordsHandled: number
  readonly recordsSkipped: number
  readonly recordsWritten: number
  readonly batchesWritten: number
  readonly errors: number
}

export interface ProgressBehavior extends RuntimeBehavior {
  getSnapshot(): ProgressSnapshot
}

export type ErrorMode = "fail-fast" | "skip-and-collect"

export type PipelineStatus = "idle" | "opening" | "running" | "aborting" | "closing" | "finished" | "failed"

export interface RunOptions {
  readonly signal?: AbortSignal
  readonly runtime?: RuntimeKind
  readonly logger?: Logger
  readonly metrics?: MetricsCollector
  readonly behaviors?: readonly RuntimeBehavior[]
  readonly errorMode?: ErrorMode
  readonly channelCapacity?: number
  readonly highWaterMark?: number
  readonly onEvent?: (event: RuntimeEvent, ctx: RuntimeContext) => MaybePromise<void>
}

export interface RunResult {
  readonly ok: boolean
  readonly metrics: Record<string, number>
  readonly errors: readonly RuntimeError[]
}

export interface RuntimeErrorDetail {
  readonly code?: RuntimeErrorCode
  readonly stage?: string
  readonly input?: unknown
  readonly cause?: unknown
  readonly metadata?: RuntimeMetadata
}

export interface DeadLetterRecord {
  readonly id: string
  readonly timestamp: number
  readonly code: RuntimeErrorCode
  readonly message: string
  readonly stage?: string
  readonly input?: unknown
  readonly metadata?: RuntimeMetadata
  readonly payload: DeadLetterPayload
}

export class RuntimeError extends Error {
  readonly code: RuntimeErrorCode
  readonly stage?: string
  readonly input?: unknown
  readonly metadata?: RuntimeMetadata
  override readonly cause?: unknown

  constructor(message: string, detail: RuntimeErrorDetail = {}) {
    super(message)
    this.name = "RuntimeError"
    this.code = detail.code ?? "RUNTIME_ERROR"
    this.stage = detail.stage
    this.input = detail.input
    this.metadata = detail.metadata
    this.cause = detail.cause
  }
}

export function createDeadLetterRecord(error: RuntimeError, payload: DeadLetterPayload = {}): DeadLetterRecord {
  return {
    id: createDeadLetterId(),
    timestamp: Date.now(),
    code: error.code,
    message: error.message,
    stage: error.stage,
    input: error.input,
    metadata: error.metadata,
    payload
  }
}

function createDeadLetterId(): string {
  return `dl_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`
}
