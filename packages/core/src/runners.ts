import {
  batchFlushEnd,
  batchFlushStart,
  channelBackpressure,
  sinkWriteEnd,
  sinkWriteStart,
  stageEnd,
  stageItemEnd,
  stageItemStart,
  stageStart
} from "./events.js"
import {
  assertNotAborted,
  emitRuntimeEvent,
  notifyBatch,
  notifyError,
  notifyRecord,
  toRuntimeError,
  type RuntimeState
} from "./runtime.js"
import {
  CORE_RUNTIME_ERROR_CODES,
  RuntimeError,
  type Batch,
  type BatchOptions,
  type Decoder,
  type Encoder,
  type ErrorMode,
  type RuntimeEvent,
  type Sink,
  type Transform
} from "./types.js"
import type { RuntimeChannel } from "./channel.js"

export interface TransformRunnerOptions<I, O> {
  readonly stage: Transform<I, O>
  readonly stageId?: string
  readonly input: RuntimeChannel<I>
  readonly output: RuntimeChannel<O>
  readonly state: RuntimeState
  readonly errorMode?: ErrorMode
  readonly manageLifecycle?: boolean
}

export interface DecoderRunnerOptions<O> {
  readonly stage: Decoder<O>
  readonly stageId?: string
  readonly input: RuntimeChannel<Uint8Array>
  readonly output: RuntimeChannel<O>
  readonly state: RuntimeState
  readonly manageLifecycle?: boolean
}

export interface EncoderRunnerOptions<I> {
  readonly stage: Encoder<I>
  readonly stageId?: string
  readonly input: RuntimeChannel<I>
  readonly output: RuntimeChannel<Uint8Array>
  readonly state: RuntimeState
  readonly manageLifecycle?: boolean
}

export interface BatchRunnerOptions<T> {
  readonly stageId?: string
  readonly input: RuntimeChannel<T>
  readonly output: RuntimeChannel<Batch<T>>
  readonly state: RuntimeState
  readonly options?: BatchOptions<T>
}

export interface SinkRunnerOptions<T> {
  readonly stageId?: string
  readonly sink: Sink<T>
  readonly input: RuntimeChannel<Batch<T>>
  readonly state: RuntimeState
  readonly manageLifecycle?: boolean
}

interface OrderedCompletion<T> {
  readonly results: readonly T[]
  readonly emitted: Promise<void>
  resolveEmitted(): void
  rejectEmitted(error: unknown): void
}

export async function runTransformRunner<I, O>(options: TransformRunnerOptions<I, O>): Promise<void> {
  const runner = new TransformRunner(options)
  await runner.run()
}

export async function runDecoderRunner<O>(options: DecoderRunnerOptions<O>): Promise<void> {
  const runner = new DecoderRunner(options)
  await runner.run()
}

export async function runEncoderRunner<I>(options: EncoderRunnerOptions<I>): Promise<void> {
  const runner = new EncoderRunner(options)
  await runner.run()
}

export async function runBatchRunner<T>(options: BatchRunnerOptions<T>): Promise<void> {
  const runner = new BatchRunner(options)
  await runner.run()
}

export async function runSinkRunner<T>(options: SinkRunnerOptions<T>): Promise<void> {
  const runner = new SinkRunner(options)
  await runner.run()
}

class TransformRunner<I, O> {
  private readonly stageId: string
  private readonly concurrency: number
  private readonly preserveOrder: boolean
  private readonly maxInFlight: number
  private readonly continueOnError: boolean
  private nextSequence = 0
  private nextToEmit = 0
  private failure: RuntimeError | undefined
  private readonly orderedCompletions = new Map<number, OrderedCompletion<O>>()
  private flushChain = Promise.resolve()

  constructor(private readonly options: TransformRunnerOptions<I, O>) {
    this.stageId = options.stageId ?? options.stage.name
    this.concurrency = Math.max(1, options.stage.concurrency ?? 1)
    this.preserveOrder = options.stage.preserveOrder ?? false
    this.maxInFlight = Math.max(1, options.stage.maxInFlight ?? this.concurrency)
    this.continueOnError = options.errorMode === "skip-and-collect"
  }

  async run(): Promise<void> {
    const { stage, state, output } = this.options
    let opened = false

    try {
      const startEmitted = emitEvent(state, stageStart({ stage: this.stageId, name: stage.name, kind: stage.kind }))
      if (startEmitted) await startEmitted
      if (this.options.manageLifecycle !== false) {
        await stage.open?.(state.ctx)
        opened = true
      }

      const workerCount = Math.min(this.concurrency, this.maxInFlight)
      const workerResults = await Promise.allSettled(Array.from({ length: workerCount }, () => this.runWorker()))
      if (this.failure) {
        await this.flushChain.catch(() => undefined)
        throw this.failure
      }

      const rejectedWorker = workerResults.find((result) => result.status === "rejected")
      if (rejectedWorker) {
        throw rejectedWorker.reason
      }

      await this.flushChain

      if (this.failure) {
        throw this.failure
      }

      output.close()
      const endEmitted = emitEvent(state, stageEnd({ stage: this.stageId, name: stage.name, kind: stage.kind }))
      if (endEmitted) await endEmitted
    } catch (error) {
      const runtimeError = toRuntimeError(error, this.stageId)
      this.fail(runtimeError)
      throw runtimeError
    } finally {
      if (opened) {
        await stage.close?.(state.ctx)
      }
    }
  }

  private async runWorker(): Promise<void> {
    while (!this.failure) {
      assertNotAborted(this.options.state.ctx)
      const pulled = await this.options.input.pull(this.options.state.ctx)
      if (pulled.done) {
        return
      }

      const sequence = this.nextSequence++
      await this.runItem(sequence, pulled.value)
    }
  }

  private async runItem(sequence: number, input: I): Promise<void> {
    const { stage, state } = this.options
    const startedAt = Date.now()
    if (state.detailedEvents) {
      const startEmitted = emitEvent(
        state,
        stageItemStart({ stage: this.stageId, name: stage.name, kind: stage.kind, input })
      )
      if (startEmitted) await startEmitted
    }

    try {
      const result = await stage.handle(input, state.ctx)
      const results = normalizeResult(result)
      const endEmitted = emitEvent(
        state,
        stageItemEnd({
          stage: this.stageId,
          name: stage.name,
          kind: stage.kind,
          input,
          durationMs: Date.now() - startedAt
        })
      )
      if (endEmitted) await endEmitted
      const recordNotified = notifyRecord(state, this.stageId, result === undefined ? "skipped" : "handled")
      if (recordNotified) await recordNotified
      await this.emitResults(sequence, results)
    } catch (error) {
      const runtimeError = this.toStageRuntimeError(error, input)
      const firstFailFastError = !this.continueOnError && !this.failure
      if (
        runtimeError.code !== CORE_RUNTIME_ERROR_CODES.pipelineAborted &&
        (this.continueOnError || firstFailFastError) &&
        !state.errors.includes(runtimeError)
      ) {
        await notifyError(state, runtimeError)
      }
      const endEmitted = emitEvent(
        state,
        stageItemEnd({
          stage: this.stageId,
          name: stage.name,
          kind: stage.kind,
          input,
          durationMs: Date.now() - startedAt
        })
      )
      if (endEmitted) await endEmitted

      if (!this.continueOnError) {
        this.fail(runtimeError)
        throw this.failure
      }

      await this.emitResults(sequence, [])
    }
  }

  private async emitResults(sequence: number, results: readonly O[]): Promise<void> {
    if (!this.preserveOrder) {
      await this.pushResults(results)
      return
    }

    const completion = createOrderedCompletion(results)
    this.orderedCompletions.set(sequence, completion)
    await this.flushOrdered()
    await completion.emitted
  }

  private flushOrdered(): Promise<void> {
    this.flushChain = this.flushChain.then(async () => {
      while (this.orderedCompletions.has(this.nextToEmit)) {
        const completion = this.orderedCompletions.get(this.nextToEmit) as OrderedCompletion<O>
        this.orderedCompletions.delete(this.nextToEmit)
        await this.pushResults(completion.results)
        completion.resolveEmitted()
        this.nextToEmit += 1
      }
    })
    return this.flushChain
  }

  private async pushResults(results: readonly O[]): Promise<void> {
    for (const result of results) {
      assertNotAborted(this.options.state.ctx)
      if (this.options.output.size >= this.options.output.capacity) {
        const emitted = emitEvent(
          this.options.state,
          channelBackpressure({
            channel: this.options.output.name,
            size: this.options.output.size,
            capacity: this.options.output.capacity,
            operation: "push"
          })
        )
        if (emitted) await emitted
      }
      await this.options.output.push(result, this.options.state.ctx)
    }
  }

  private fail(error: RuntimeError): void {
    if (this.failure) {
      return
    }
    this.failure = error
    this.rejectOrderedCompletions(error)
    this.options.input.fail(error)
    this.options.output.fail(error)
  }

  private rejectOrderedCompletions(error: RuntimeError): void {
    for (const completion of this.orderedCompletions.values()) {
      completion.rejectEmitted(error)
    }
    this.orderedCompletions.clear()
  }

  private toStageRuntimeError(error: unknown, input: I): RuntimeError {
    if (error instanceof RuntimeError && error.stage !== undefined) {
      return error
    }

    const runtimeError = toRuntimeError(error, this.stageId, input)
    return new RuntimeError(runtimeError.message, {
      code: runtimeError.code,
      stage: this.stageId,
      input,
      cause: runtimeError.cause,
      metadata: {
        ...runtimeError.metadata,
        stageName: this.options.stage.name
      }
    })
  }
}

function normalizeResult<T>(result: T | T[] | undefined): readonly T[] {
  if (result === undefined) {
    return []
  }
  return Array.isArray(result) ? result : [result]
}

function createOrderedCompletion<T>(results: readonly T[]): OrderedCompletion<T> {
  let resolveEmitted: () => void = () => {}
  let rejectEmitted: (error: unknown) => void = () => {}
  const emitted = new Promise<void>((resolve, reject) => {
    resolveEmitted = resolve
    rejectEmitted = reject
  })
  void emitted.catch(() => undefined)
  return { results, emitted, resolveEmitted, rejectEmitted }
}

class DecoderRunner<O> {
  private readonly stageId: string

  constructor(private readonly options: DecoderRunnerOptions<O>) {
    this.stageId = options.stageId ?? options.stage.name
  }

  async run(): Promise<void> {
    const { stage, state, output } = this.options
    let opened = false

    try {
      const startEmitted = emitEvent(state, stageStart({ stage: this.stageId, name: stage.name, kind: stage.kind }))
      if (startEmitted) await startEmitted
      if (this.options.manageLifecycle !== false) {
        await stage.open?.(state.ctx)
        opened = true
      }

      const decoded = stage.decode(channelToIterable(this.options.input, state), state.ctx)
      const iterator = decoded[Symbol.asyncIterator]()

      while (true) {
        assertNotAborted(state.ctx)
        const startedAt = Date.now()
        const pulled = await iterator.next()
        if (pulled.done) {
          break
        }
        if (state.detailedEvents) {
          const itemStartEmitted = emitEvent(
            state,
            stageItemStart({ stage: this.stageId, name: stage.name, kind: stage.kind })
          )
          if (itemStartEmitted) await itemStartEmitted
        }
        const itemEndEmitted = emitEvent(
          state,
          stageItemEnd({
            stage: this.stageId,
            name: stage.name,
            kind: stage.kind,
            durationMs: Date.now() - startedAt
          })
        )
        if (itemEndEmitted) await itemEndEmitted
        const recordNotified = notifyRecord(state, this.stageId, "handled")
        if (recordNotified) await recordNotified
        await output.push(pulled.value, state.ctx)
      }

      output.close()
      const endEmitted = emitEvent(state, stageEnd({ stage: this.stageId, name: stage.name, kind: stage.kind }))
      if (endEmitted) await endEmitted
    } catch (error) {
      const runtimeError = this.toCodecRuntimeError(error)
      await this.notifyFailure(runtimeError)
      this.options.input.fail(runtimeError)
      output.fail(runtimeError)
      throw runtimeError
    } finally {
      if (opened) {
        await stage.close?.(state.ctx)
      }
    }
  }

  private toCodecRuntimeError(error: unknown): RuntimeError {
    if (error instanceof RuntimeError && error.stage !== undefined) {
      return error
    }

    const runtimeError = toRuntimeError(error, this.stageId)
    return new RuntimeError(runtimeError.message, {
      code: runtimeError.code,
      stage: this.stageId,
      cause: runtimeError.cause,
      metadata: {
        ...runtimeError.metadata,
        stageName: this.options.stage.name
      }
    })
  }

  private async notifyFailure(error: RuntimeError): Promise<void> {
    if (error.code !== CORE_RUNTIME_ERROR_CODES.pipelineAborted && !this.options.state.errors.includes(error)) {
      await notifyError(this.options.state, error)
    }
  }
}

class EncoderRunner<I> {
  private readonly stageId: string

  constructor(private readonly options: EncoderRunnerOptions<I>) {
    this.stageId = options.stageId ?? options.stage.name
  }

  async run(): Promise<void> {
    const { stage, state, output } = this.options
    let opened = false

    try {
      const startEmitted = emitEvent(state, stageStart({ stage: this.stageId, name: stage.name, kind: stage.kind }))
      if (startEmitted) await startEmitted
      if (this.options.manageLifecycle !== false) {
        await stage.open?.(state.ctx)
        opened = true
      }

      const encoded = stage.encode(channelToIterable(this.options.input, state), state.ctx)
      const iterator = encoded[Symbol.asyncIterator]()

      while (true) {
        assertNotAborted(state.ctx)
        const startedAt = Date.now()
        const pulled = await iterator.next()
        if (pulled.done) {
          break
        }
        if (state.detailedEvents) {
          const itemStartEmitted = emitEvent(
            state,
            stageItemStart({ stage: this.stageId, name: stage.name, kind: stage.kind })
          )
          if (itemStartEmitted) await itemStartEmitted
        }
        const itemEndEmitted = emitEvent(
          state,
          stageItemEnd({
            stage: this.stageId,
            name: stage.name,
            kind: stage.kind,
            durationMs: Date.now() - startedAt
          })
        )
        if (itemEndEmitted) await itemEndEmitted
        const recordNotified = notifyRecord(state, this.stageId, "handled")
        if (recordNotified) await recordNotified
        await output.push(pulled.value, state.ctx)
      }

      output.close()
      const endEmitted = emitEvent(state, stageEnd({ stage: this.stageId, name: stage.name, kind: stage.kind }))
      if (endEmitted) await endEmitted
    } catch (error) {
      const runtimeError = this.toCodecRuntimeError(error)
      await this.notifyFailure(runtimeError)
      this.options.input.fail(runtimeError)
      output.fail(runtimeError)
      throw runtimeError
    } finally {
      if (opened) {
        await stage.close?.(state.ctx)
      }
    }
  }

  private toCodecRuntimeError(error: unknown): RuntimeError {
    if (error instanceof RuntimeError && error.stage !== undefined) {
      return error
    }

    const runtimeError = toRuntimeError(error, this.stageId)
    return new RuntimeError(runtimeError.message, {
      code: runtimeError.code,
      stage: this.stageId,
      cause: runtimeError.cause,
      metadata: {
        ...runtimeError.metadata,
        stageName: this.options.stage.name
      }
    })
  }

  private async notifyFailure(error: RuntimeError): Promise<void> {
    if (error.code !== CORE_RUNTIME_ERROR_CODES.pipelineAborted && !this.options.state.errors.includes(error)) {
      await notifyError(this.options.state, error)
    }
  }
}

async function* channelToIterable<T>(channel: RuntimeChannel<T>, state: RuntimeState): AsyncIterable<T> {
  while (true) {
    assertNotAborted(state.ctx)
    const pulled = await channel.pull(state.ctx)
    if (pulled.done) {
      return
    }
    yield pulled.value
  }
}

class BatchRunner<T> {
  private readonly stageId: string
  private readonly maxSize: number
  private readonly maxWaitMs: number | undefined
  private readonly maxBytes: number | undefined
  private readonly estimateBytes: ((item: T) => number) | undefined
  private current: T[] = []
  private currentBytes = 0
  private batchId = 0
  private pendingPull: Promise<IteratorResult<T>> | undefined

  constructor(private readonly runnerOptions: BatchRunnerOptions<T>) {
    this.stageId = runnerOptions.stageId ?? "batch"
    this.maxSize = Math.max(1, runnerOptions.options?.size ?? 1)
    this.maxWaitMs = runnerOptions.options?.maxWaitMs
    this.maxBytes = runnerOptions.options?.maxBytes
    this.estimateBytes = runnerOptions.options?.estimateBytes
  }

  async run(): Promise<void> {
    const { state, input, output } = this.runnerOptions
    const startEmitted = emitEvent(state, stageStart({ stage: this.stageId, name: "batch", kind: "batch" }))
    if (startEmitted) await startEmitted

    try {
      while (true) {
        assertNotAborted(state.ctx)
        const pulled = await this.pullNext(input)
        if (pulled === "timeout") {
          await this.flush()
          continue
        }
        if (pulled.done) {
          break
        }

        const item = pulled.value
        const bytes = this.estimateBytes?.(item) ?? 0
        if (this.maxBytes !== undefined && this.current.length > 0 && this.currentBytes + bytes > this.maxBytes) {
          await this.flush()
        }

        this.current.push(item)
        this.currentBytes += bytes

        if (this.current.length >= this.maxSize) {
          await this.flush()
        }
      }

      await this.flush()
      output.close()
      const endEmitted = emitEvent(state, stageEnd({ stage: this.stageId, name: "batch", kind: "batch" }))
      if (endEmitted) await endEmitted
    } catch (error) {
      const runtimeError = toRuntimeError(error, this.stageId)
      output.fail(runtimeError)
      throw runtimeError
    }
  }

  private async pullNext(input: RuntimeChannel<T>): Promise<IteratorResult<T> | "timeout"> {
    this.pendingPull ??= input.pull(this.runnerOptions.state.ctx)

    if (this.current.length === 0 || this.maxWaitMs === undefined) {
      const result = await this.pendingPull
      this.pendingPull = undefined
      return result
    }

    const result = await Promise.race([
      this.pendingPull,
      delay(this.maxWaitMs, this.runnerOptions.state.ctx.signal).then(() => "timeout" as const)
    ])
    if (result !== "timeout") {
      this.pendingPull = undefined
    }
    return result
  }

  private async flush(): Promise<void> {
    if (this.current.length === 0) {
      return
    }

    const startedAt = Date.now()
    const startEmitted = emitEvent(this.runnerOptions.state, batchFlushStart(this.stageId))
    if (startEmitted) await startEmitted
    const batch: Batch<T> = {
      id: this.batchId++,
      items: this.current,
      size: this.current.length,
      createdAt: Date.now(),
      bytes: this.estimateBytes ? this.currentBytes : undefined
    }
    this.current = []
    this.currentBytes = 0

    await this.pushBatch(batch)
    const endEmitted = emitEvent(this.runnerOptions.state, batchFlushEnd(this.stageId, batch, Date.now() - startedAt))
    if (endEmitted) await endEmitted
  }

  private async pushBatch(batch: Batch<T>): Promise<void> {
    const { state, output } = this.runnerOptions
    if (output.size >= output.capacity) {
      const emitted = emitEvent(
        state,
        channelBackpressure({
          channel: output.name,
          size: output.size,
          capacity: output.capacity,
          operation: "push"
        })
      )
      if (emitted) await emitted
    }
    await output.push(batch, state.ctx)
  }
}

class SinkRunner<T> {
  private readonly stageId: string

  constructor(private readonly options: SinkRunnerOptions<T>) {
    this.stageId = options.stageId ?? options.sink.name
  }

  async run(): Promise<void> {
    const { sink, state, input } = this.options
    let opened = false
    const startEmitted = emitEvent(state, stageStart({ stage: this.stageId, name: sink.name, kind: sink.kind }))
    if (startEmitted) await startEmitted

    try {
      if (this.options.manageLifecycle !== false) {
        await sink.open?.(state.ctx)
        opened = true
      }

      while (true) {
        assertNotAborted(state.ctx)
        const pulled = await input.pull(state.ctx)
        if (pulled.done) {
          break
        }
        await this.writeBatch(pulled.value)
      }

      const endEmitted = emitEvent(state, stageEnd({ stage: this.stageId, name: sink.name, kind: sink.kind }))
      if (endEmitted) await endEmitted
    } catch (error) {
      const runtimeError = toRuntimeError(error, this.stageId)
      input.fail(runtimeError)
      throw runtimeError
    } finally {
      if (opened) {
        await sink.close?.(state.ctx)
      }
    }
  }

  private async writeBatch(batch: Batch<T>): Promise<void> {
    const { sink, state } = this.options
    const startedAt = Date.now()
    const startEmitted = emitEvent(state, sinkWriteStart(this.stageId, batch))
    if (startEmitted) await startEmitted

    try {
      await sink.write(batch, state.ctx)
      const endEmitted = emitEvent(state, sinkWriteEnd(this.stageId, batch, Date.now() - startedAt))
      if (endEmitted) await endEmitted
      const batchNotified = notifyBatch(state, batch, sink.name)
      if (batchNotified) await batchNotified
    } catch (error) {
      const runtimeError = toRuntimeError(error, this.stageId, batch)
      await notifyError(state, runtimeError)
      throw runtimeError
    }
  }
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(
        new RuntimeError("Pipeline aborted", { code: CORE_RUNTIME_ERROR_CODES.pipelineAborted, cause: signal.reason })
      )
      return
    }

    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    const onAbort = (): void => {
      clearTimeout(timer)
      signal.removeEventListener("abort", onAbort)
      reject(
        new RuntimeError("Pipeline aborted", { code: CORE_RUNTIME_ERROR_CODES.pipelineAborted, cause: signal.reason })
      )
    }
    signal.addEventListener("abort", onAbort, { once: true })
  })
}

function emitEvent(state: RuntimeState, event: RuntimeEvent): Promise<void> | undefined {
  return emitRuntimeEvent(state.ctx, event)
}
