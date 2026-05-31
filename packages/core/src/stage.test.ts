import { describe, expect, it } from "vitest"
import { map } from "./transforms.js"
import { buildInternalStages } from "./stage.js"
import type { Decoder, Encoder, Sink, Source } from "./types.js"

describe("buildInternalStages", () => {
  it("builds internal stages in pipeline order", () => {
    const stages = buildInternalStages({
      source: source("file"),
      stages: [map((value: number) => value + 1), decoder("jsonl"), encoder("bytes")],
      batchOptions: { size: 100 },
      sink: sink("writer")
    })

    expect(stages.map((stage) => stage.kind)).toEqual(["source", "transform", "decoder", "encoder", "batch", "sink"])
    expect(stages.map((stage) => stage.id)).toEqual(["file#1", "map#1", "jsonl#1", "bytes#1", "batch#1", "writer#1"])
  })

  it("generates stable ids for repeated stage names", () => {
    const stages = buildInternalStages({
      source: source("map"),
      stages: [map((value: number) => value, { name: "map" }), map((value: number) => value, { name: "map" })],
      sink: sink("map")
    })

    expect(stages.map((stage) => stage.id)).toEqual(["map#1", "map#2", "map#3", "map#4"])
  })

  it("carries transform scheduling options without changing defaults", () => {
    const stages = buildInternalStages({
      source: source("source"),
      stages: [
        map((value: number) => value, {
          concurrency: 4,
          preserveOrder: true,
          maxInFlight: 16
        })
      ],
      sink: sink("sink")
    })

    expect(stages[1]).toMatchObject({
      kind: "transform",
      concurrency: 4,
      preserveOrder: true,
      maxInFlight: 16
    })
  })

  it("defaults transform preserveOrder to the current unordered concurrent behavior", () => {
    const stages = buildInternalStages({
      source: source("source"),
      stages: [map((value: number) => value, { concurrency: 2 })],
      sink: sink("sink")
    })

    expect(stages[1]).toMatchObject({
      kind: "transform",
      concurrency: 2,
      preserveOrder: false
    })
  })
})

function source<T>(name: string): Source<T> {
  return {
    kind: "source",
    name,
    async *read() {}
  }
}

function sink<T>(name: string): Sink<T> {
  return {
    kind: "sink",
    name,
    write() {}
  }
}

function decoder<T>(name: string): Decoder<T> {
  return {
    kind: "decoder",
    name,
    async *decode() {}
  }
}

function encoder<T>(name: string): Encoder<T> {
  return {
    kind: "encoder",
    name,
    async *encode() {}
  }
}
