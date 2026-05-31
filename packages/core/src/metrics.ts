import type { MetricsCollector, RuntimeEvent } from "./types.js"

interface ChannelMetricKeys {
  readonly push: string
  readonly pull: string
  readonly wait: string
  readonly waitPush: string
  readonly waitPull: string
  readonly waitMs: string
  readonly backpressure: string
  readonly size: string
}

interface StageMetricKeys {
  readonly starts: string
  readonly ends: string
  readonly durationMs: string
  readonly itemStarts: string
  readonly items: string
  readonly itemTotalMs: string
  readonly itemMaxMs: string
  readonly waits: string
  readonly waitTotalMs: string
  readonly waitPrefix: string
  readonly waitByOperation: Map<string, string>
  readonly read: string
  readonly handled: string
  readonly skipped: string
  readonly written: string
}

interface BatchMetricKeys {
  readonly flushStarts: string
  readonly flushes: string
  readonly records: string
  readonly flushTotalMs: string
  readonly flushMaxMs: string
}

interface SinkMetricKeys {
  readonly writeStarts: string
  readonly writes: string
  readonly records: string
  readonly writeTotalMs: string
  readonly writeMaxMs: string
}

const channelMetricKeys = new Map<string, ChannelMetricKeys>()
const stageMetricKeys = new Map<string, StageMetricKeys>()
const batchMetricKeys = new Map<string, BatchMetricKeys>()
const sinkMetricKeys = new Map<string, SinkMetricKeys>()
const recordMetricKeys = {
  read: "records.read",
  handled: "records.handled",
  skipped: "records.skipped",
  written: "records.written"
}
const batchSummaryMetricKeys = {
  batchesWritten: "batches.written",
  bytesWritten: "bytes.written"
}

export class InMemoryMetricsCollector implements MetricsCollector {
  private readonly values = new Map<string, number>()

  increment(name: string, value = 1): void {
    this.values.set(name, (this.values.get(name) ?? 0) + value)
  }

  observe(name: string, value: number): void {
    this.values.set(name, value)
  }

  get(name: string): number | undefined {
    return this.values.get(name)
  }

  observeMax(name: string, value: number): void {
    this.values.set(name, Math.max(this.values.get(name) ?? Number.NEGATIVE_INFINITY, value))
  }

  snapshot(): Record<string, number> {
    return Object.fromEntries(this.values)
  }
}

export function applyMetricEvent(metrics: MetricsCollector, event: RuntimeEvent): void {
  if (event.type === "record") {
    const keys = getStageMetricKeys(event.stage)
    metrics.increment(recordMetricKeys[event.action], event.count)
    metrics.increment(keys[event.action], event.count)
    return
  }

  if (event.type === "batch") {
    applyBatchMetrics(metrics, event.batch)
    return
  }

  if (event.type === "stage.start") {
    metrics.increment(getStageMetricKeys(event.stage).starts)
    return
  }

  if (event.type === "stage.end") {
    const keys = getStageMetricKeys(event.stage)
    metrics.increment(keys.ends)
    if (event.durationMs !== undefined) {
      metrics.observe(keys.durationMs, event.durationMs)
    }
    return
  }

  if (event.type === "stage.item.start") {
    metrics.increment(getStageMetricKeys(event.stage).itemStarts)
    return
  }

  if (event.type === "stage.item.end") {
    const keys = getStageMetricKeys(event.stage)
    metrics.increment(keys.items)
    if (event.durationMs !== undefined) {
      metrics.increment(keys.itemTotalMs, event.durationMs)
      observeMax(metrics, keys.itemMaxMs, event.durationMs)
    }
    return
  }

  if (event.type === "stage.wait") {
    const keys = getStageMetricKeys(event.stage)
    metrics.increment(keys.waits)
    metrics.increment(keys.waitTotalMs, event.durationMs)
    if (event.operation) {
      metrics.increment(getStageWaitOperationKey(keys, event.operation))
    }
    return
  }

  if (event.type === "channel.push" || event.type === "channel.pull") {
    const keys = getChannelMetricKeys(event.channel)
    metrics.increment(event.type === "channel.push" ? keys.push : keys.pull)
    metrics.observe(keys.size, event.size)
    return
  }

  if (event.type === "channel.wait" || event.type === "channel.backpressure") {
    const keys = getChannelMetricKeys(event.channel)
    if (event.type === "channel.backpressure") {
      metrics.increment(keys.backpressure)
    }
    metrics.increment(keys.wait)
    if (event.operation) {
      metrics.increment(event.operation === "push" ? keys.waitPush : keys.waitPull)
    }
    if (event.durationMs !== undefined) {
      metrics.observe(keys.waitMs, event.durationMs)
    }
    metrics.observe(keys.size, event.size)
    return
  }

  if (event.type === "sink.write.start") {
    metrics.increment(getSinkMetricKeys(event.stage).writeStarts)
    return
  }

  if (event.type === "sink.write.end") {
    const keys = getSinkMetricKeys(event.stage)
    metrics.increment(keys.writes)
    metrics.increment(keys.records, event.batch.size)
    if (event.durationMs !== undefined) {
      metrics.increment(keys.writeTotalMs, event.durationMs)
      observeMax(metrics, keys.writeMaxMs, event.durationMs)
    }
    return
  }

  if (event.type === "batch.flush.start") {
    metrics.increment(getBatchMetricKeys(event.stage).flushStarts)
    return
  }

  if (event.type === "batch.flush.end") {
    const keys = getBatchMetricKeys(event.stage)
    metrics.increment(keys.flushes)
    if (event.batch) {
      metrics.increment(keys.records, event.batch.size)
    }
    if (event.durationMs !== undefined) {
      metrics.increment(keys.flushTotalMs, event.durationMs)
      observeMax(metrics, keys.flushMaxMs, event.durationMs)
    }
    return
  }

  if (event.type === "error") {
    metrics.increment("errors")
  }
}

function getChannelMetricKeys(channel: string): ChannelMetricKeys {
  const cached = channelMetricKeys.get(channel)
  if (cached) {
    return cached
  }

  const prefix = `channel.${channel}`
  const keys = {
    push: `${prefix}.push`,
    pull: `${prefix}.pull`,
    wait: `${prefix}.wait`,
    waitPush: `${prefix}.wait.push`,
    waitPull: `${prefix}.wait.pull`,
    waitMs: `${prefix}.waitMs`,
    backpressure: `${prefix}.backpressure`,
    size: `${prefix}.size`
  }
  channelMetricKeys.set(channel, keys)
  return keys
}

export function applyRecordMetrics(
  metrics: MetricsCollector,
  stage: string,
  action: "read" | "handled" | "skipped" | "written",
  count = 1
): void {
  const keys = getStageMetricKeys(stage)
  metrics.increment(recordMetricKeys[action], count)
  metrics.increment(keys[action], count)
}

export function applyBatchMetrics(
  metrics: MetricsCollector,
  batch: { readonly size: number; readonly bytes?: number }
): void {
  metrics.increment(batchSummaryMetricKeys.batchesWritten)
  metrics.increment(recordMetricKeys.written, batch.size)
  if (batch.bytes !== undefined) {
    metrics.increment(batchSummaryMetricKeys.bytesWritten, batch.bytes)
  }
}

function getStageMetricKeys(stage: string): StageMetricKeys {
  const cached = stageMetricKeys.get(stage)
  if (cached) {
    return cached
  }

  const prefix = `stage.${stage}`
  const keys = {
    starts: `${prefix}.starts`,
    ends: `${prefix}.ends`,
    durationMs: `${prefix}.durationMs`,
    itemStarts: `${prefix}.item.starts`,
    items: `${prefix}.items`,
    itemTotalMs: `${prefix}.item.totalMs`,
    itemMaxMs: `${prefix}.item.maxMs`,
    waits: `${prefix}.waits`,
    waitTotalMs: `${prefix}.wait.totalMs`,
    waitPrefix: `${prefix}.wait.`,
    waitByOperation: new Map<string, string>(),
    read: `${prefix}.read`,
    handled: `${prefix}.handled`,
    skipped: `${prefix}.skipped`,
    written: `${prefix}.written`
  }
  stageMetricKeys.set(stage, keys)
  return keys
}

function getStageWaitOperationKey(keys: StageMetricKeys, operation: string): string {
  const cached = keys.waitByOperation.get(operation)
  if (cached) {
    return cached
  }

  const key = `${keys.waitPrefix}${operation}`
  keys.waitByOperation.set(operation, key)
  return key
}

function getBatchMetricKeys(stage: string): BatchMetricKeys {
  const cached = batchMetricKeys.get(stage)
  if (cached) {
    return cached
  }

  const prefix = `batch.${stage}`
  const keys = {
    flushStarts: `${prefix}.flush.starts`,
    flushes: `${prefix}.flushes`,
    records: `${prefix}.records`,
    flushTotalMs: `${prefix}.flush.totalMs`,
    flushMaxMs: `${prefix}.flush.maxMs`
  }
  batchMetricKeys.set(stage, keys)
  return keys
}

function getSinkMetricKeys(stage: string): SinkMetricKeys {
  const cached = sinkMetricKeys.get(stage)
  if (cached) {
    return cached
  }

  const prefix = `sink.${stage}`
  const keys = {
    writeStarts: `${prefix}.write.starts`,
    writes: `${prefix}.writes`,
    records: `${prefix}.records`,
    writeTotalMs: `${prefix}.write.totalMs`,
    writeMaxMs: `${prefix}.write.maxMs`
  }
  sinkMetricKeys.set(stage, keys)
  return keys
}

function observeMax(metrics: MetricsCollector, name: string, value: number): void {
  if (metrics instanceof InMemoryMetricsCollector) {
    metrics.observeMax(name, value)
    return
  }

  metrics.observe(name, Math.max(metrics.snapshot()[name] ?? Number.NEGATIVE_INFINITY, value))
}
