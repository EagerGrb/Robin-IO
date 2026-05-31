import { createPipelineLifecycle } from "./lifecycle.js"
import { executePipeline } from "./scheduler.js"
import {
  assertNotAborted,
  createRuntimeState,
  notifyError,
  notifyFinish,
  notifyStart,
  toRuntimeError
} from "./runtime.js"
import { CORE_RUNTIME_ERROR_CODES, RuntimeError } from "./types.js"
import type {
  BatchOptions,
  Decoder,
  Encoder,
  PipelineStage,
  RunOptions,
  RunResult,
  PipelineStatus,
  Sink,
  Source,
  Transform
} from "./types.js"

export function pipeline(): PipelineBuilder<never> {
  return PipelineBuilder.create()
}

export interface PipelinePlugin<I, O = I> {
  readonly name: string
  readonly version?: string
  configure(builder: PipelineBuilder<I>): PipelineBuilder<O>
}

export class PipelineBuilder<TCurrent> {
  private constructor(
    private readonly source?: Source<unknown>,
    private readonly stages: readonly PipelineStage[] = [],
    private readonly batchOptions: BatchOptions | undefined = undefined
  ) {}

  static create(): PipelineBuilder<never> {
    return new PipelineBuilder<never>()
  }

  from<T>(source: Source<T>): PipelineBuilder<T> {
    return new PipelineBuilder<T>(source, this.stages, this.batchOptions)
  }

  through<TNext>(stage: Transform<TCurrent, TNext>): PipelineBuilder<TNext>
  through<TNext>(this: PipelineBuilder<Uint8Array>, stage: Decoder<TNext>): PipelineBuilder<TNext>
  through(stage: Encoder<TCurrent>): PipelineBuilder<Uint8Array>
  through<TNext>(stage: PipelineStage<TCurrent, TNext>): PipelineBuilder<TNext> {
    return new PipelineBuilder<TNext>(this.source, [...this.stages, stage as PipelineStage], this.batchOptions)
  }

  use<TNext>(plugin: PipelinePlugin<TCurrent, TNext>): PipelineBuilder<TNext> {
    return plugin.configure(this)
  }

  batch(options: BatchOptions<TCurrent>): PipelineBuilder<TCurrent> {
    return new PipelineBuilder<TCurrent>(this.source, this.stages, options as BatchOptions)
  }

  to(sink: Sink<TCurrent>): PipelineTask<TCurrent> {
    if (!this.source) {
      throw new Error("Pipeline source is required. Call from(source) before to(sink).")
    }
    return new PipelineTask<TCurrent>(
      this.source as Source<unknown>,
      this.stages,
      this.batchOptions,
      sink as Sink<unknown>
    )
  }
}

export class PipelineTask<TOut> {
  private readonly lifecycle = createPipelineLifecycle()
  private currentState: ReturnType<typeof createRuntimeState> | undefined

  constructor(
    private readonly source: Source<unknown>,
    private readonly stages: readonly PipelineStage[],
    private readonly batchOptions: BatchOptions | undefined,
    private readonly sink: Sink<unknown>
  ) {}

  get status(): PipelineStatus {
    return this.lifecycle.status
  }

  cancel(
    reason: unknown = new RuntimeError("Pipeline aborted", { code: CORE_RUNTIME_ERROR_CODES.pipelineAborted })
  ): void {
    const status = this.lifecycle.status
    if (status === "finished" || status === "failed") {
      return
    }

    if (status === "idle") {
      this.lifecycle.transition("opening")
    }
    const nextStatus = this.lifecycle.status
    if (nextStatus !== "aborting" && nextStatus !== "closing") {
      this.lifecycle.transition("aborting")
    }
    this.currentState?.ctx.abort(reason)
  }

  async run(options: RunOptions = {}): Promise<RunResult> {
    if (this.status !== "idle") {
      throw new RuntimeError("Pipeline task can only be run once.", {
        code: CORE_RUNTIME_ERROR_CODES.pipelineAlreadyRun,
        metadata: { status: this.status }
      })
    }

    this.lifecycle.transition("opening")
    const state = createRuntimeState(options)
    this.currentState = state
    const openedStages: PipelineStage[] = []
    let sourceOpened = false
    let sinkOpened = false
    let pipelineCompleted = false

    try {
      await notifyStart(state)
      await this.source.open?.(state.ctx)
      sourceOpened = true
      for (const stage of this.stages) {
        await stage.open?.(state.ctx)
        openedStages.push(stage)
      }
      await this.sink.open?.(state.ctx)
      sinkOpened = true
      this.lifecycle.transition("running")

      await executePipeline({
        source: this.source,
        stages: this.stages,
        batchOptions: this.batchOptions,
        sink: this.sink,
        runOptions: options,
        state
      })
      assertNotAborted(state.ctx)

      state.ctx.metadata.set("pipeline.failed", false)
      pipelineCompleted = true
    } catch (error) {
      state.ctx.metadata.set("pipeline.failed", true)
      const runtimeError = toRuntimeError(error)
      if (runtimeError.code === CORE_RUNTIME_ERROR_CODES.pipelineAborted && this.lifecycle.status !== "aborting") {
        this.lifecycle.transition("aborting")
      }
      if (!state.errors.includes(runtimeError)) {
        await notifyError(state, runtimeError)
      }
    } finally {
      if (this.lifecycle.status !== "closing") {
        this.lifecycle.transition("closing")
      }
      if (sinkOpened) {
        await closeResource(() => this.sink.close?.(state.ctx), this.sink.name, state)
      }
      for (const stage of [...openedStages].reverse()) {
        await closeResource(() => stage.close?.(state.ctx), stage.name, state)
      }
      if (sourceOpened) {
        await closeResource(() => this.source.close?.(state.ctx), this.source.name, state)
      }
      if (this.lifecycle.beginFinish()) {
        await notifyFinish(state)
      }
      this.lifecycle.markTerminal(!pipelineCompleted || state.errors.length > 0)
      this.currentState = undefined
    }

    return {
      ok: pipelineCompleted && state.errors.length === 0,
      metrics: state.ctx.metrics.snapshot(),
      errors: [...state.errors]
    }
  }
}

async function closeResource(
  close: () => Promise<void> | void | undefined,
  stage: string,
  state: ReturnType<typeof createRuntimeState>
): Promise<void> {
  try {
    await close()
  } catch (error) {
    await notifyError(state, toRuntimeError(error, stage))
  }
}
