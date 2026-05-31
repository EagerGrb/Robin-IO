import type {
  Batch,
  BatchFlushEvent,
  ChannelEvent,
  InternalRuntimeEventBase,
  SinkWriteEvent,
  StageEvent,
  StageItemEvent,
  StageWaitEvent
} from "./types.js"

export interface StageEventInput {
  readonly stage: string
  readonly name?: string
  readonly kind?: string
  readonly durationMs?: number
  readonly metadata?: InternalRuntimeEventBase["metadata"]
}

export interface StageItemEventInput extends StageEventInput {
  readonly input?: unknown
}

export interface StageWaitEventInput extends StageEventInput {
  readonly operation?: string
  readonly durationMs: number
}

export interface ChannelEventInput {
  readonly channel: string
  readonly size: number
  readonly capacity: number
  readonly operation?: "push" | "pull"
  readonly durationMs?: number
  readonly metadata?: InternalRuntimeEventBase["metadata"]
}

export function stageStart(input: StageEventInput): StageEvent {
  return createStageEvent("stage.start", input)
}

export function stageEnd(input: StageEventInput): StageEvent {
  return createStageEvent("stage.end", input)
}

export function stageItemStart(input: StageItemEventInput): StageItemEvent {
  return createStageItemEvent("stage.item.start", input)
}

export function stageItemEnd(input: StageItemEventInput): StageItemEvent {
  return createStageItemEvent("stage.item.end", input)
}

export function stageWait(input: StageWaitEventInput): StageWaitEvent {
  return {
    type: "stage.wait",
    stage: input.stage,
    name: input.name,
    kind: input.kind,
    operation: input.operation,
    durationMs: input.durationMs,
    timestamp: Date.now(),
    metadata: input.metadata
  }
}

export function channelPush(input: ChannelEventInput): ChannelEvent {
  return createChannelEvent("channel.push", input)
}

export function channelPull(input: ChannelEventInput): ChannelEvent {
  return createChannelEvent("channel.pull", input)
}

export function channelWait(input: ChannelEventInput): ChannelEvent {
  return createChannelEvent("channel.wait", input)
}

export function channelBackpressure(input: ChannelEventInput): ChannelEvent {
  return createChannelEvent("channel.backpressure", input)
}

export function sinkWriteStart(stage: string, batch: Batch<unknown>): SinkWriteEvent {
  return { type: "sink.write.start", stage, batch, timestamp: Date.now() }
}

export function sinkWriteEnd(stage: string, batch: Batch<unknown>, durationMs: number): SinkWriteEvent {
  return { type: "sink.write.end", stage, batch, durationMs, timestamp: Date.now() }
}

export function batchFlushStart(stage: string): BatchFlushEvent {
  return { type: "batch.flush.start", stage, timestamp: Date.now() }
}

export function batchFlushEnd(stage: string, batch: Batch<unknown>, durationMs: number): BatchFlushEvent {
  return { type: "batch.flush.end", stage, batch, durationMs, timestamp: Date.now() }
}

function createStageEvent(type: StageEvent["type"], input: StageEventInput): StageEvent {
  return {
    type,
    stage: input.stage,
    name: input.name,
    kind: input.kind,
    durationMs: input.durationMs,
    timestamp: Date.now(),
    metadata: input.metadata
  }
}

function createStageItemEvent(type: StageItemEvent["type"], input: StageItemEventInput): StageItemEvent {
  return {
    type,
    stage: input.stage,
    name: input.name,
    kind: input.kind,
    input: input.input,
    durationMs: input.durationMs,
    timestamp: Date.now(),
    metadata: input.metadata
  }
}

function createChannelEvent(type: ChannelEvent["type"], input: ChannelEventInput): ChannelEvent {
  return {
    type,
    channel: input.channel,
    size: input.size,
    capacity: input.capacity,
    operation: input.operation,
    durationMs: input.durationMs,
    timestamp: Date.now(),
    metadata: input.metadata
  }
}
