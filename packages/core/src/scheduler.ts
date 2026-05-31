import { createRuntimeChannel, type RuntimeChannel } from "./channel.js"
import {
  batchFlushEnd,
  batchFlushStart,
  sinkWriteEnd,
  sinkWriteStart,
  stageEnd,
  stageItemEnd,
  stageStart
} from "./events.js"
import { runBatchRunner, runDecoderRunner, runEncoderRunner, runSinkRunner } from "./runners.js"
import { buildInternalStages, type InternalStage } from "./stage.js"
import { applyTransform } from "./transforms.js"
import {
  assertNotAborted,
  emitRuntimeEvent,
  notifyBatch,
  notifyError,
  notifyRecord,
  toRuntimeError,
  type RuntimeState
} from "./runtime.js"
import type {
  Batch,
  BatchOptions,
  Decoder,
  Encoder,
  PipelineStage,
  RunOptions,
  RuntimeContext,
  Sink,
  Source,
  Transform
} from "./types.js"
import { CORE_RUNTIME_ERROR_CODES, RuntimeError } from "./types.js"

export interface PipelineSchedulerOptions {
  readonly source: Source<unknown>
  readonly stages: readonly PipelineStage[]
  readonly batchOptions?: BatchOptions
  readonly sink: Sink<unknown>
  readonly runOptions: RunOptions
  readonly state: RuntimeState
}

export async function executePipeline(options: PipelineSchedulerOptions): Promise<void> {
  const capacity = getChannelCapacity(options.runOptions)
  const internalStages = buildInternalStages(options)
  const sourceStage = internalStages[0]
  const pipelineStages = internalStages.slice(1, 1 + options.stages.length)
  const batchStage = internalStages.find((stage) => stage.kind === "batch")
  const sinkStage = internalStages[internalStages.length - 1]
  const sourceChannel = createRuntimeChannel<unknown>({ name: `${sourceStage.id}:source`, capacity })
  const useDirectSource = canUseDirectSource(options.state)
  const sourceRunner = useDirectSource
    ? undefined
    : runSourceRunner(options.source, sourceStage, sourceChannel, options.state).catch(() => undefined)

  try {
    let stream = useDirectSource
      ? readSourceDirect(options.source, sourceStage, options.state)
      : channelToIterable(sourceChannel, options.state)
    const continueOnError = options.runOptions.errorMode === "skip-and-collect"

    for (const [index, stage] of options.stages.entries()) {
      const internalStage = pipelineStages[index]
      assertNotAborted(options.state.ctx)
      if (stage.kind === "transform") {
        stream = applyTransform(
          stream,
          stage as Transform<unknown, unknown>,
          options.state,
          continueOnError,
          internalStage.id
        )
      } else if (stage.kind === "decoder") {
        stream = applyDecoder(
          stream as AsyncIterable<Uint8Array>,
          stage as Decoder<unknown>,
          internalStage,
          options.state,
          capacity
        )
      } else {
        stream = applyEncoder(stream, stage as Encoder<unknown>, internalStage, options.state, capacity)
      }
    }

    await runPipelineOutput(
      stream,
      options.batchOptions,
      options.sink,
      options.state,
      capacity,
      batchStage?.id,
      sinkStage.id
    )
    if (sourceRunner) {
      await sourceRunner
    }
  } catch (error) {
    const runtimeError = toRuntimeError(error)
    sourceChannel.fail(runtimeError)
    throw runtimeError
  }
}

async function* readSourceDirect(
  source: Source<unknown>,
  stage: InternalStage,
  state: RuntimeState
): AsyncIterable<unknown> {
  const iterator = source.read(state.ctx)[Symbol.asyncIterator]()
  try {
    while (true) {
      assertNotAborted(state.ctx)
      const pulled = await pullSourceNext(iterator, state.ctx)
      if (pulled.done) {
        return
      }
      const item = pulled.value
      const recordNotified = notifyRecord(state, stage.id, "read")
      if (recordNotified) await recordNotified
      yield item
    }
  } catch (error) {
    throw toStageRuntimeError(error, stage.id, source.name)
  } finally {
    if (state.ctx.signal.aborted) {
      void Promise.resolve(iterator.return?.()).catch(() => undefined)
    }
  }
}

function pullSourceNext<T>(iterator: AsyncIterator<T>, ctx: RuntimeContext): Promise<IteratorResult<T>> {
  const next = iterator.next()
  if (ctx.signal.aborted) {
    return Promise.reject(
      new RuntimeError("Pipeline aborted", { code: CORE_RUNTIME_ERROR_CODES.pipelineAborted, cause: ctx.signal.reason })
    )
  }
  const aborted = new Promise<never>((_, reject) => {
    const onAbort = (): void => {
      ctx.signal.removeEventListener("abort", onAbort)
      reject(
        new RuntimeError("Pipeline aborted", {
          code: CORE_RUNTIME_ERROR_CODES.pipelineAborted,
          cause: ctx.signal.reason
        })
      )
    }
    ctx.signal.addEventListener("abort", onAbort, { once: true })
    void next.then(
      () => ctx.signal.removeEventListener("abort", onAbort),
      () => ctx.signal.removeEventListener("abort", onAbort)
    )
  })
  return Promise.race([next, aborted])
}

async function runSourceRunner(
  source: Source<unknown>,
  stage: InternalStage,
  output: RuntimeChannel<unknown>,
  state: RuntimeState
): Promise<void> {
  try {
    for await (const item of source.read(state.ctx)) {
      assertNotAborted(state.ctx)
      const recordNotified = notifyRecord(state, stage.id, "read")
      if (recordNotified) await recordNotified
      await output.push(item, state.ctx)
    }
    output.close()
  } catch (error) {
    const runtimeError = toStageRuntimeError(error, stage.id, source.name)
    output.fail(runtimeError)
    throw runtimeError
  }
}

function toStageRuntimeError(error: unknown, stageId: string, stageName: string): RuntimeError {
  const runtimeError = toRuntimeError(error, stageId)
  if (runtimeError.stage === stageId) {
    return runtimeError
  }

  return new RuntimeError(runtimeError.message, {
    code: runtimeError.code,
    stage: stageId,
    input: runtimeError.input,
    cause: runtimeError.cause,
    metadata: {
      ...runtimeError.metadata,
      stageName
    }
  })
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

async function* applyDecoder<T>(
  input: AsyncIterable<Uint8Array>,
  stage: Decoder<T>,
  internalStage: InternalStage,
  state: RuntimeState,
  capacity: number
): AsyncIterable<T> {
  if (canUseDirectCodec(state)) {
    yield* applyDirectDecoder(input, stage, internalStage, state)
    return
  }

  const inputChannel = createRuntimeChannel<Uint8Array>({ name: `${internalStage.id}:input`, capacity })
  const outputChannel = createRuntimeChannel<T>({ name: `${internalStage.id}:output`, capacity })
  const feeder = feedChannel(input, inputChannel, state).catch(() => undefined)
  const runner = runDecoderRunner({
    stage,
    stageId: internalStage.id,
    input: inputChannel,
    output: outputChannel,
    state,
    manageLifecycle: false
  }).catch(() => undefined)

  try {
    yield* channelToIterable(outputChannel, state)
    await runner
    await feeder
  } finally {
    await Promise.allSettled([runner, feeder])
  }
}

async function* applyEncoder<T>(
  input: AsyncIterable<T>,
  stage: Encoder<T>,
  internalStage: InternalStage,
  state: RuntimeState,
  capacity: number
): AsyncIterable<Uint8Array> {
  if (canUseDirectCodec(state)) {
    yield* applyDirectEncoder(input, stage, internalStage, state)
    return
  }

  const inputChannel = createRuntimeChannel<T>({ name: `${internalStage.id}:input`, capacity })
  const outputChannel = createRuntimeChannel<Uint8Array>({ name: `${internalStage.id}:output`, capacity })
  const feeder = feedChannel(input, inputChannel, state).catch(() => undefined)
  const runner = runEncoderRunner({
    stage,
    stageId: internalStage.id,
    input: inputChannel,
    output: outputChannel,
    state,
    manageLifecycle: false
  }).catch(() => undefined)

  try {
    yield* channelToIterable(outputChannel, state)
    await runner
    await feeder
  } finally {
    await Promise.allSettled([runner, feeder])
  }
}

async function* applyDirectDecoder<T>(
  input: AsyncIterable<Uint8Array>,
  stage: Decoder<T>,
  internalStage: InternalStage,
  state: RuntimeState
): AsyncIterable<T> {
  const startEmitted = emitRuntimeEvent(
    state.ctx,
    stageStart({ stage: internalStage.id, name: stage.name, kind: stage.kind })
  )
  if (startEmitted) await startEmitted

  try {
    const decoded = stage.decode(input, state.ctx)
    for await (const item of decoded) {
      assertNotAborted(state.ctx)
      const startedAt = Date.now()
      const itemEndEmitted = emitRuntimeEvent(
        state.ctx,
        stageItemEnd({
          stage: internalStage.id,
          name: stage.name,
          kind: stage.kind,
          durationMs: Date.now() - startedAt
        })
      )
      if (itemEndEmitted) await itemEndEmitted
      const recordNotified = notifyRecord(state, internalStage.id, "handled")
      if (recordNotified) await recordNotified
      yield item
    }

    const endEmitted = emitRuntimeEvent(
      state.ctx,
      stageEnd({ stage: internalStage.id, name: stage.name, kind: stage.kind })
    )
    if (endEmitted) await endEmitted
  } catch (error) {
    const runtimeError = toCodecRuntimeError(error, internalStage.id, stage.name)
    if (runtimeError.code !== CORE_RUNTIME_ERROR_CODES.pipelineAborted && !state.errors.includes(runtimeError)) {
      await notifyError(state, runtimeError)
    }
    throw runtimeError
  }
}

async function* applyDirectEncoder<T>(
  input: AsyncIterable<T>,
  stage: Encoder<T>,
  internalStage: InternalStage,
  state: RuntimeState
): AsyncIterable<Uint8Array> {
  const startEmitted = emitRuntimeEvent(
    state.ctx,
    stageStart({ stage: internalStage.id, name: stage.name, kind: stage.kind })
  )
  if (startEmitted) await startEmitted

  try {
    const encoded = stage.encode(input, state.ctx)
    for await (const chunk of encoded) {
      assertNotAborted(state.ctx)
      const startedAt = Date.now()
      const itemEndEmitted = emitRuntimeEvent(
        state.ctx,
        stageItemEnd({
          stage: internalStage.id,
          name: stage.name,
          kind: stage.kind,
          durationMs: Date.now() - startedAt
        })
      )
      if (itemEndEmitted) await itemEndEmitted
      const recordNotified = notifyRecord(state, internalStage.id, "handled")
      if (recordNotified) await recordNotified
      yield chunk
    }

    const endEmitted = emitRuntimeEvent(
      state.ctx,
      stageEnd({ stage: internalStage.id, name: stage.name, kind: stage.kind })
    )
    if (endEmitted) await endEmitted
  } catch (error) {
    const runtimeError = toCodecRuntimeError(error, internalStage.id, stage.name)
    if (runtimeError.code !== CORE_RUNTIME_ERROR_CODES.pipelineAborted && !state.errors.includes(runtimeError)) {
      await notifyError(state, runtimeError)
    }
    throw runtimeError
  }
}

function toCodecRuntimeError(error: unknown, stageId: string, stageName: string): RuntimeError {
  if (error instanceof RuntimeError && error.stage !== undefined) {
    return error
  }

  const runtimeError = toRuntimeError(error, stageId)
  return new RuntimeError(runtimeError.message, {
    code: runtimeError.code,
    stage: stageId,
    cause: runtimeError.cause,
    metadata: {
      ...runtimeError.metadata,
      stageName
    }
  })
}

async function runPipelineOutput(
  stream: AsyncIterable<unknown>,
  batchOptions: BatchOptions | undefined,
  sink: Sink<unknown>,
  state: RuntimeState,
  capacity: number,
  batchStageId: string | undefined,
  sinkStageId: string
): Promise<void> {
  if (canUseFusedOutput(batchOptions, state)) {
    await runFusedPipelineOutput(stream, batchOptions, sink, state, batchStageId ?? "batch", sinkStageId)
    return
  }

  const itemChannel = createRuntimeChannel<unknown>({ name: "pipeline:items", capacity })
  const batchChannel = createRuntimeChannel<Batch<unknown>>({ name: "pipeline:batches", capacity })
  const feeder = feedChannel(stream, itemChannel, state).catch(() => undefined)
  const batchRunner = runBatchRunner({
    stageId: batchStageId ?? "batch",
    input: itemChannel,
    output: batchChannel,
    state,
    options: batchOptions
  }).catch(() => undefined)

  try {
    await runSinkRunner({
      stageId: sinkStageId,
      sink,
      input: batchChannel,
      state,
      manageLifecycle: false
    })
    await Promise.allSettled([batchRunner, feeder])
  } catch (error) {
    const runtimeError = toRuntimeError(error)
    itemChannel.fail(runtimeError)
    batchChannel.fail(runtimeError)
    await Promise.allSettled([batchRunner, feeder])
    throw runtimeError
  }
}

async function runFusedPipelineOutput(
  stream: AsyncIterable<unknown>,
  batchOptions: BatchOptions | undefined,
  sink: Sink<unknown>,
  state: RuntimeState,
  batchStageId: string,
  sinkStageId: string
): Promise<void> {
  const batcher = new FusedBatchWriter(batchOptions, sink, state, batchStageId, sinkStageId)
  await batcher.run(stream)
}

class FusedBatchWriter {
  private readonly maxSize: number
  private readonly maxBytes: number | undefined
  private readonly estimateBytes: ((item: unknown) => number) | undefined
  private current: unknown[] = []
  private currentBytes = 0
  private batchId = 0

  constructor(
    options: BatchOptions | undefined,
    private readonly sink: Sink<unknown>,
    private readonly state: RuntimeState,
    private readonly batchStageId: string,
    private readonly sinkStageId: string
  ) {
    this.maxSize = Math.max(1, options?.size ?? 1)
    this.maxBytes = options?.maxBytes
    this.estimateBytes = options?.estimateBytes
  }

  async run(stream: AsyncIterable<unknown>): Promise<void> {
    const batchStartEmitted = emitRuntimeEvent(
      this.state.ctx,
      stageStart({ stage: this.batchStageId, name: "batch", kind: "batch" })
    )
    if (batchStartEmitted) await batchStartEmitted
    const sinkStartEmitted = emitRuntimeEvent(
      this.state.ctx,
      stageStart({ stage: this.sinkStageId, name: this.sink.name, kind: this.sink.kind })
    )
    if (sinkStartEmitted) await sinkStartEmitted

    try {
      for await (const item of stream) {
        assertNotAborted(this.state.ctx)
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
      const batchEndEmitted = emitRuntimeEvent(
        this.state.ctx,
        stageEnd({ stage: this.batchStageId, name: "batch", kind: "batch" })
      )
      if (batchEndEmitted) await batchEndEmitted
      const sinkEndEmitted = emitRuntimeEvent(
        this.state.ctx,
        stageEnd({ stage: this.sinkStageId, name: this.sink.name, kind: this.sink.kind })
      )
      if (sinkEndEmitted) await sinkEndEmitted
    } catch (error) {
      throw toRuntimeError(error)
    }
  }

  private async flush(): Promise<void> {
    if (this.current.length === 0) {
      return
    }

    const startedAt = Date.now()
    const startEmitted = emitRuntimeEvent(this.state.ctx, batchFlushStart(this.batchStageId))
    if (startEmitted) await startEmitted
    const batch: Batch<unknown> = {
      id: this.batchId++,
      items: this.current,
      size: this.current.length,
      createdAt: Date.now(),
      bytes: this.estimateBytes ? this.currentBytes : undefined
    }
    this.current = []
    this.currentBytes = 0

    await this.writeBatch(batch)
    const endEmitted = emitRuntimeEvent(this.state.ctx, batchFlushEnd(this.batchStageId, batch, Date.now() - startedAt))
    if (endEmitted) await endEmitted
  }

  private async writeBatch(batch: Batch<unknown>): Promise<void> {
    const startedAt = Date.now()
    const startEmitted = emitRuntimeEvent(this.state.ctx, sinkWriteStart(this.sinkStageId, batch))
    if (startEmitted) await startEmitted

    try {
      await this.sink.write(batch, this.state.ctx)
      const endEmitted = emitRuntimeEvent(this.state.ctx, sinkWriteEnd(this.sinkStageId, batch, Date.now() - startedAt))
      if (endEmitted) await endEmitted
      const batchNotified = notifyBatch(this.state, batch, this.sink.name)
      if (batchNotified) await batchNotified
    } catch (error) {
      const runtimeError = toRuntimeError(error, this.sinkStageId, batch)
      await notifyError(this.state, runtimeError)
      throw runtimeError
    }
  }
}

function canUseFusedOutput(batchOptions: BatchOptions | undefined, state: RuntimeState): boolean {
  return !state.detailedEvents && batchOptions?.maxWaitMs === undefined
}

function canUseDirectSource(state: RuntimeState): boolean {
  return !state.detailedEvents
}

function canUseDirectCodec(state: RuntimeState): boolean {
  return !state.detailedEvents
}

async function feedChannel<T>(
  stream: AsyncIterable<T>,
  itemChannel: RuntimeChannel<T>,
  state: RuntimeState
): Promise<void> {
  try {
    for await (const item of stream) {
      assertNotAborted(state.ctx)
      await itemChannel.push(item, state.ctx)
    }
    itemChannel.close()
  } catch (error) {
    itemChannel.fail(toRuntimeError(error))
    throw error
  }
}

function getChannelCapacity(options: RunOptions): number {
  const value = options.channelCapacity ?? options.highWaterMark ?? 16
  return Number.isInteger(value) && value > 0 ? value : 16
}
