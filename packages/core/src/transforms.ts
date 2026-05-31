import { createRuntimeChannel } from "./channel.js"
import { stageEnd, stageItemEnd, stageItemStart, stageStart } from "./events.js"
import { runTransformRunner } from "./runners.js"
import {
  assertNotAborted,
  emitRuntimeEvent,
  notifyError,
  notifyRecord,
  toRuntimeError,
  type RuntimeState
} from "./runtime.js"
import {
  CORE_RUNTIME_ERROR_CODES,
  RuntimeError,
  type RuntimeContext,
  type StageOptions,
  type Transform
} from "./types.js"

export interface TransformOptions extends StageOptions {}

export function transform<I, O>(handler: Transform<I, O>["handle"], options: TransformOptions = {}): Transform<I, O> {
  return {
    kind: "transform",
    name: options.name ?? "transform",
    concurrency: options.concurrency,
    preserveOrder: options.preserveOrder,
    maxInFlight: options.maxInFlight,
    handle: handler
  }
}

export function map<I, O>(
  mapper: (input: I, ctx: RuntimeContext) => O | Promise<O>,
  options: TransformOptions = {}
): Transform<I, O> {
  return transform((input, ctx) => mapper(input, ctx), {
    name: options.name ?? "map",
    concurrency: options.concurrency,
    preserveOrder: options.preserveOrder,
    maxInFlight: options.maxInFlight
  })
}

export function filter<T>(
  predicate: (input: T, ctx: RuntimeContext) => boolean | Promise<boolean>,
  options: TransformOptions = {}
): Transform<T, T> {
  return transform(async (input, ctx) => ((await predicate(input, ctx)) ? input : undefined), {
    name: options.name ?? "filter",
    concurrency: options.concurrency,
    preserveOrder: options.preserveOrder,
    maxInFlight: options.maxInFlight
  })
}

export function validate<T>(
  predicate: (input: T, ctx: RuntimeContext) => boolean | Promise<boolean>,
  message = "Validation failed",
  options: TransformOptions = {}
): Transform<T, T> {
  return transform(
    async (input, ctx) => {
      if (await predicate(input, ctx)) {
        return input
      }
      throw new Error(message)
    },
    {
      name: options.name ?? "validate",
      concurrency: options.concurrency,
      preserveOrder: options.preserveOrder,
      maxInFlight: options.maxInFlight
    }
  )
}

export async function* applyTransform<I, O>(
  input: AsyncIterable<I>,
  stage: Transform<I, O>,
  state: RuntimeState,
  continueOnError: boolean,
  stageId = stage.name
): AsyncIterable<O> {
  if (canUseDirectTransform(stage, state)) {
    yield* applyDirectTransform(input, stage, state, continueOnError, stageId)
    return
  }

  const concurrency = Math.max(1, stage.concurrency ?? 1)
  const capacity = Math.max(1, stage.maxInFlight ?? concurrency)
  const inputChannel = createRuntimeChannel<I>({ name: `${stageId}:input`, capacity })
  const outputChannel = createRuntimeChannel<O>({ name: `${stageId}:output`, capacity })

  const feeder = feedTransformInput(input, inputChannel, state).catch(() => undefined)
  const runner = runTransformRunner({
    stage,
    stageId,
    input: inputChannel,
    output: outputChannel,
    state,
    errorMode: continueOnError ? "skip-and-collect" : "fail-fast",
    manageLifecycle: false
  }).catch(() => undefined)

  try {
    while (true) {
      assertNotAborted(state.ctx)
      const result = await outputChannel.pull(state.ctx)
      if (result.done) {
        break
      }
      yield result.value
    }

    await runner
    await feeder
  } finally {
    await Promise.allSettled([runner, feeder])
  }
}

async function* applyDirectTransform<I, O>(
  input: AsyncIterable<I>,
  stage: Transform<I, O>,
  state: RuntimeState,
  continueOnError: boolean,
  stageId: string
): AsyncIterable<O> {
  const startEmitted = emitRuntimeEvent(state.ctx, stageStart({ stage: stageId, name: stage.name, kind: stage.kind }))
  if (startEmitted) await startEmitted

  for await (const item of input) {
    assertNotAborted(state.ctx)
    const results = await handleDirectTransformItem(item, stage, state, continueOnError, stageId)
    for (const result of results) {
      assertNotAborted(state.ctx)
      yield result
    }
  }

  const endEmitted = emitRuntimeEvent(state.ctx, stageEnd({ stage: stageId, name: stage.name, kind: stage.kind }))
  if (endEmitted) await endEmitted
}

async function handleDirectTransformItem<I, O>(
  input: I,
  stage: Transform<I, O>,
  state: RuntimeState,
  continueOnError: boolean,
  stageId: string
): Promise<readonly O[]> {
  const startedAt = Date.now()
  if (state.detailedEvents) {
    const startEmitted = emitRuntimeEvent(
      state.ctx,
      stageItemStart({ stage: stageId, name: stage.name, kind: stage.kind, input })
    )
    if (startEmitted) await startEmitted
  }

  try {
    const result = await stage.handle(input, state.ctx)
    const endEmitted = emitRuntimeEvent(
      state.ctx,
      stageItemEnd({ stage: stageId, name: stage.name, kind: stage.kind, input, durationMs: Date.now() - startedAt })
    )
    if (endEmitted) await endEmitted
    const recordNotified = notifyRecord(state, stageId, result === undefined ? "skipped" : "handled")
    if (recordNotified) await recordNotified
    return normalizeResult(result)
  } catch (error) {
    const runtimeError = toTransformRuntimeError(error, stage, stageId, input)
    if (runtimeError.code !== CORE_RUNTIME_ERROR_CODES.pipelineAborted) {
      await notifyError(state, runtimeError)
    }
    const endEmitted = emitRuntimeEvent(
      state.ctx,
      stageItemEnd({ stage: stageId, name: stage.name, kind: stage.kind, input, durationMs: Date.now() - startedAt })
    )
    if (endEmitted) await endEmitted

    if (!continueOnError) {
      throw runtimeError
    }

    return []
  }
}

function canUseDirectTransform<I, O>(stage: Transform<I, O>, state: RuntimeState): boolean {
  return (
    !state.detailedEvents &&
    (stage.concurrency === undefined || stage.concurrency === 1) &&
    (stage.maxInFlight === undefined || stage.maxInFlight === 1) &&
    stage.preserveOrder !== true
  )
}

function normalizeResult<T>(result: T | T[] | undefined): readonly T[] {
  if (result === undefined) {
    return []
  }
  return Array.isArray(result) ? result : [result]
}

function toTransformRuntimeError<I, O>(
  error: unknown,
  stage: Transform<I, O>,
  stageId: string,
  input: I
): RuntimeError {
  if (error instanceof RuntimeError && error.stage !== undefined) {
    return error
  }

  const runtimeError = toRuntimeError(error, stageId, input)
  return new RuntimeError(runtimeError.message, {
    code: runtimeError.code,
    stage: stageId,
    input,
    cause: runtimeError.cause,
    metadata: {
      ...runtimeError.metadata,
      stageName: stage.name
    }
  })
}

async function feedTransformInput<I>(
  input: AsyncIterable<I>,
  inputChannel: ReturnType<typeof createRuntimeChannel<I>>,
  state: RuntimeState
): Promise<void> {
  try {
    for await (const item of input) {
      assertNotAborted(state.ctx)
      await inputChannel.push(item, state.ctx)
    }
    inputChannel.close()
  } catch (error) {
    inputChannel.fail(toRuntimeError(error))
    throw error
  }
}
