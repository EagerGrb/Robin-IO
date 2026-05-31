import { describe, expect, it } from "vitest"
import {
  batchFlushEnd,
  batchFlushStart,
  channelBackpressure,
  channelPull,
  channelPush,
  channelWait,
  sinkWriteEnd,
  sinkWriteStart,
  stageEnd,
  stageItemEnd,
  stageItemStart,
  stageStart,
  stageWait
} from "./events.js"
import { applyMetricEvent, InMemoryMetricsCollector } from "./metrics.js"
import type { Batch, RuntimeEvent } from "./types.js"

describe("metrics event mapping", () => {
  it("records stage, channel, sink, and batch metrics from runtime events", () => {
    const metrics = new InMemoryMetricsCollector()
    const batch = createBatch([1, 2, 3])
    const events: RuntimeEvent[] = [
      stageStart({ stage: "map#1", name: "map", kind: "transform" }),
      stageItemStart({ stage: "map#1" }),
      stageItemEnd({ stage: "map#1", durationMs: 4 }),
      stageItemStart({ stage: "map#1" }),
      stageItemEnd({ stage: "map#1", durationMs: 7 }),
      stageWait({ stage: "map#1", operation: "output", durationMs: 3 }),
      stageEnd({ stage: "map#1", durationMs: 15 }),
      channelPush({ channel: "source-map", size: 1, capacity: 2 }),
      channelPull({ channel: "source-map", size: 0, capacity: 2 }),
      channelWait({ channel: "source-map", size: 2, capacity: 2, operation: "push", durationMs: 5 }),
      channelBackpressure({ channel: "source-map", size: 2, capacity: 2, operation: "push", durationMs: 6 }),
      batchFlushStart("batch#1"),
      batchFlushEnd("batch#1", batch, 2),
      sinkWriteStart("sink#1", batch),
      sinkWriteEnd("sink#1", batch, 9)
    ]

    for (const event of events) {
      applyMetricEvent(metrics, event)
    }

    expect(metrics.snapshot()).toMatchObject({
      "stage.map#1.starts": 1,
      "stage.map#1.item.starts": 2,
      "stage.map#1.items": 2,
      "stage.map#1.item.totalMs": 11,
      "stage.map#1.item.maxMs": 7,
      "stage.map#1.waits": 1,
      "stage.map#1.wait.totalMs": 3,
      "stage.map#1.wait.output": 1,
      "stage.map#1.ends": 1,
      "stage.map#1.durationMs": 15,
      "channel.source-map.push": 1,
      "channel.source-map.pull": 1,
      "channel.source-map.wait": 2,
      "channel.source-map.wait.push": 2,
      "channel.source-map.backpressure": 1,
      "channel.source-map.waitMs": 6,
      "channel.source-map.size": 2,
      "batch.batch#1.flush.starts": 1,
      "batch.batch#1.flushes": 1,
      "batch.batch#1.records": 3,
      "batch.batch#1.flush.totalMs": 2,
      "sink.sink#1.write.starts": 1,
      "sink.sink#1.writes": 1,
      "sink.sink#1.records": 3,
      "sink.sink#1.write.totalMs": 9
    })
  })

  it("tracks max values without changing metric semantics", () => {
    const metrics = new InMemoryMetricsCollector()

    metrics.observeMax("stage.map#1.item.maxMs", 4)
    metrics.observeMax("stage.map#1.item.maxMs", 2)
    metrics.observeMax("stage.map#1.item.maxMs", 7)

    expect(metrics.get("stage.map#1.item.maxMs")).toBe(7)
    expect(metrics.snapshot()).toMatchObject({
      "stage.map#1.item.maxMs": 7
    })
  })
})

function createBatch<T>(items: readonly T[]): Batch<T> {
  return {
    id: 0,
    items,
    size: items.length,
    createdAt: Date.now()
  }
}
