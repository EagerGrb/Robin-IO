import { assertNotAborted, emitRuntimeEvent, toRuntimeError } from "./runtime.js"
import { CORE_RUNTIME_ERROR_CODES, RuntimeError, type RuntimeContext, type RuntimeEvent } from "./types.js"

export interface RuntimeChannelOptions {
  readonly name: string
  readonly capacity: number
  readonly lowWaterMark?: number
}

export interface RuntimeChannel<T> {
  readonly name: string
  readonly size: number
  readonly capacity: number
  push(item: T, ctx: RuntimeContext): Promise<void>
  pull(ctx: RuntimeContext): Promise<IteratorResult<T>>
  close(): void
  fail(error: RuntimeError): void
}

interface WaitingPush<T> {
  readonly item: T
  readonly ctx: RuntimeContext
  readonly resolve: () => void
  readonly reject: (error: RuntimeError) => void
  readonly cleanup: () => void
}

interface WaitingPull<T> {
  readonly ctx: RuntimeContext
  readonly resolve: (result: IteratorResult<T>) => void
  readonly reject: (error: RuntimeError) => void
  readonly cleanup: () => void
}

export function createRuntimeChannel<T>(options: RuntimeChannelOptions): RuntimeChannel<T> {
  if (!Number.isInteger(options.capacity) || options.capacity < 1) {
    throw new RangeError("RuntimeChannel capacity must be a positive integer.")
  }

  return new BoundedRuntimeChannel<T>(options)
}

class BoundedRuntimeChannel<T> implements RuntimeChannel<T> {
  readonly name: string
  readonly capacity: number
  readonly lowWaterMark?: number
  private readonly pushMetric: string
  private readonly pullMetric: string
  private readonly sizeMetric: string
  private readonly waitMetric: string
  private readonly waitPushMetric: string
  private readonly waitPullMetric: string
  private readonly queue: T[] = []
  private readonly waitingPushes: WaitingPush<T>[] = []
  private readonly waitingPulls: WaitingPull<T>[] = []
  private closed = false
  private failed: RuntimeError | undefined

  constructor(options: RuntimeChannelOptions) {
    this.name = options.name
    this.capacity = options.capacity
    this.lowWaterMark = options.lowWaterMark
    this.pushMetric = `channel.${this.name}.push`
    this.pullMetric = `channel.${this.name}.pull`
    this.sizeMetric = `channel.${this.name}.size`
    this.waitMetric = `channel.${this.name}.wait`
    this.waitPushMetric = `channel.${this.name}.wait.push`
    this.waitPullMetric = `channel.${this.name}.wait.pull`
  }

  get size(): number {
    return this.queue.length
  }

  async push(item: T, ctx: RuntimeContext): Promise<void> {
    const state = getRuntimeDetails(ctx)
    assertNotAborted(ctx)
    this.throwIfFailed()
    if (this.closed) {
      throw new RuntimeError(`Channel ${this.name} is closed`, {
        code: CORE_RUNTIME_ERROR_CODES.channelClosed,
        metadata: { channel: this.name }
      })
    }

    if (this.waitingPulls.length > 0) {
      const pull = this.waitingPulls.shift()
      pull?.cleanup()
      pull?.resolve({ done: false, value: item })
      const emitted = this.emitPush(ctx, state)
      if (emitted) await emitted
      return
    }

    if (this.queue.length < this.capacity) {
      this.queue.push(item)
      const emitted = this.emitPush(ctx, state)
      if (emitted) await emitted
      return
    }

    const startedAt = Date.now()
    await new Promise<void>((resolve, reject) => {
      const waiting = createWaitingPush(item, ctx, resolve, reject, () => {
        removeItem(this.waitingPushes, waiting)
      })
      this.waitingPushes.push(waiting)
    })
    const emitted = this.emitWait(ctx, state, "push", Date.now() - startedAt)
    if (emitted) await emitted
  }

  async pull(ctx: RuntimeContext): Promise<IteratorResult<T>> {
    const state = getRuntimeDetails(ctx)
    assertNotAborted(ctx)
    this.throwIfFailed()

    if (this.queue.length > 0) {
      const value = this.queue.shift() as T
      await this.drainWaitingPushes()
      const emitted = this.emitPull(ctx, state)
      if (emitted) await emitted
      return { done: false, value }
    }

    if (this.closed) {
      return { done: true, value: undefined }
    }

    const startedAt = Date.now()
    const result = await new Promise<IteratorResult<T>>((resolve, reject) => {
      const waiting = createWaitingPull(ctx, resolve, reject, () => {
        removeItem(this.waitingPulls, waiting)
      })
      this.waitingPulls.push(waiting)
    })
    const waitEmitted = this.emitWait(ctx, state, "pull", Date.now() - startedAt)
    if (waitEmitted) await waitEmitted
    if (!result.done) {
      const pullEmitted = this.emitPull(ctx, state)
      if (pullEmitted) await pullEmitted
    }
    return result
  }

  close(): void {
    if (this.closed || this.failed) {
      return
    }

    this.closed = true
    this.resolveDonePullsIfDrained()
  }

  fail(error: RuntimeError): void {
    if (this.failed) {
      return
    }

    this.failed = error
    for (const push of this.waitingPushes.splice(0)) {
      push.cleanup()
      push.reject(error)
    }
    for (const pull of this.waitingPulls.splice(0)) {
      pull.cleanup()
      pull.reject(error)
    }
  }

  private async drainWaitingPushes(): Promise<void> {
    while (!this.failed && !this.closed && this.waitingPushes.length > 0 && this.queue.length < this.capacity) {
      const push = this.waitingPushes.shift() as WaitingPush<T>
      push.cleanup()

      if (push.ctx.signal.aborted) {
        push.reject(abortError(push.ctx))
        continue
      }

      if (this.waitingPulls.length > 0) {
        const pull = this.waitingPulls.shift()
        pull?.cleanup()
        pull?.resolve({ done: false, value: push.item })
      } else {
        this.queue.push(push.item)
      }

      try {
        const emitted = this.emitPush(push.ctx, getRuntimeDetails(push.ctx))
        if (emitted) await emitted
        push.resolve()
      } catch (error) {
        const runtimeError = toRuntimeError(error)
        push.reject(runtimeError)
        throw runtimeError
      }
    }

    this.resolveDonePullsIfDrained()
  }

  private resolveDonePullsIfDrained(): void {
    if (!this.closed || this.queue.length > 0) {
      return
    }

    for (const pull of this.waitingPulls.splice(0)) {
      pull.cleanup()
      pull.resolve({ done: true, value: undefined })
    }
  }

  private throwIfFailed(): void {
    if (this.failed) {
      throw this.failed
    }
  }

  private emitPush(ctx: RuntimeContext, state: RuntimeDetails): Promise<void> | undefined {
    if (!state.detailedChannelEvents) {
      ctx.metrics.increment(this.pushMetric)
      ctx.metrics.observe(this.sizeMetric, this.queue.length)
      return undefined
    }

    return this.emit(ctx, {
      type: "channel.push",
      channel: this.name,
      size: this.queue.length,
      capacity: this.capacity,
      timestamp: Date.now()
    })
  }

  private emitPull(ctx: RuntimeContext, state: RuntimeDetails): Promise<void> | undefined {
    if (!state.detailedChannelEvents) {
      ctx.metrics.increment(this.pullMetric)
      ctx.metrics.observe(this.sizeMetric, this.queue.length)
      return undefined
    }

    return this.emit(ctx, {
      type: "channel.pull",
      channel: this.name,
      size: this.queue.length,
      capacity: this.capacity,
      timestamp: Date.now()
    })
  }

  private emitWait(
    ctx: RuntimeContext,
    state: RuntimeDetails,
    operation: "push" | "pull",
    durationMs: number
  ): Promise<void> | undefined {
    if (!state.detailedChannelEvents) {
      ctx.metrics.increment(this.waitMetric)
      ctx.metrics.increment(operation === "push" ? this.waitPushMetric : this.waitPullMetric)
      ctx.metrics.observe(`${this.waitMetric}Ms`, durationMs)
      ctx.metrics.observe(this.sizeMetric, this.queue.length)
      return undefined
    }

    return this.emit(ctx, {
      type: "channel.wait",
      channel: this.name,
      operation,
      durationMs,
      size: this.queue.length,
      capacity: this.capacity,
      timestamp: Date.now()
    })
  }

  private emit(ctx: RuntimeContext, event: RuntimeEvent): Promise<void> | undefined {
    return emitRuntimeEvent(ctx, event)
  }
}

function createWaitingPush<T>(
  item: T,
  ctx: RuntimeContext,
  resolve: () => void,
  reject: (error: RuntimeError) => void,
  remove: () => void
): WaitingPush<T> {
  const onAbort = (): void => {
    remove()
    ctx.signal.removeEventListener("abort", onAbort)
    reject(abortError(ctx))
  }
  ctx.signal.addEventListener("abort", onAbort, { once: true })
  return {
    item,
    ctx,
    resolve,
    reject,
    cleanup() {
      ctx.signal.removeEventListener("abort", onAbort)
    }
  }
}

function createWaitingPull<T>(
  ctx: RuntimeContext,
  resolve: (result: IteratorResult<T>) => void,
  reject: (error: RuntimeError) => void,
  remove: () => void
): WaitingPull<T> {
  const onAbort = (): void => {
    remove()
    ctx.signal.removeEventListener("abort", onAbort)
    reject(abortError(ctx))
  }
  ctx.signal.addEventListener("abort", onAbort, { once: true })
  return {
    ctx,
    resolve,
    reject,
    cleanup() {
      ctx.signal.removeEventListener("abort", onAbort)
    }
  }
}

function abortError(ctx: RuntimeContext): RuntimeError {
  return new RuntimeError("Pipeline aborted", {
    code: CORE_RUNTIME_ERROR_CODES.pipelineAborted,
    cause: ctx.signal.reason
  })
}

function removeItem<T>(items: T[], item: T): void {
  const index = items.indexOf(item)
  if (index >= 0) {
    items.splice(index, 1)
  }
}

interface RuntimeDetails {
  readonly detailedChannelEvents?: boolean
}

function getRuntimeDetails(ctx: RuntimeContext): RuntimeDetails {
  return ctx
}
