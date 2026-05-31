import { describe, expect, it } from "vitest"
import { createRuntimeState } from "./runtime.js"
import { executePipeline } from "./scheduler.js"
import { map } from "./transforms.js"
import type { Batch, Sink, Source } from "./types.js"

describe("executePipeline", () => {
  it("runs source -> map -> batch -> sink", async () => {
    const state = createRuntimeState()
    const output: number[] = []

    await executePipeline({
      source: source([1, 2, 3]),
      stages: [map((value: number) => value * 2)],
      batchOptions: { size: 2 },
      sink: collectingSink(output),
      runOptions: {},
      state
    })

    expect(output).toEqual([2, 4, 6])
  })

  it("limits upstream reads when sink is slow", async () => {
    const state = createRuntimeState()
    let readCount = 0
    let firstWriteStarted: (() => void) | undefined
    const firstWrite = new Promise<void>((resolve) => {
      firstWriteStarted = resolve
    })
    let releaseSink: (() => void) | undefined
    const sinkReleased = new Promise<void>((resolve) => {
      releaseSink = resolve
    })

    const run = executePipeline({
      source: {
        kind: "source",
        name: "numbers",
        async *read() {
          for (let value = 0; value < 100; value += 1) {
            readCount += 1
            yield value
          }
        }
      },
      stages: [],
      batchOptions: { size: 1 },
      sink: {
        kind: "sink",
        name: "slow",
        async write() {
          firstWriteStarted?.()
          await sinkReleased
        }
      },
      runOptions: { channelCapacity: 4 },
      state
    })

    await firstWrite
    await delay(10)
    expect(readCount).toBeLessThan(20)
    releaseSink?.()
    await run
    expect(readCount).toBe(100)
  })

  it("fails fast for transform errors and continues in skip-and-collect", async () => {
    const failFastState = createRuntimeState()
    await expect(
      executePipeline({
        source: source([1, 2, 3]),
        stages: [
          map((value: number) => {
            if (value === 2) {
              throw new Error("bad transform")
            }
            return value
          })
        ],
        sink: collectingSink([]),
        runOptions: { errorMode: "fail-fast" },
        state: failFastState
      })
    ).rejects.toMatchObject({ stage: "map#1" })

    const skipState = createRuntimeState()
    const output: number[] = []
    await executePipeline({
      source: source([1, 2, 3]),
      stages: [
        map((value: number) => {
          if (value === 2) {
            throw new Error("bad transform")
          }
          return value
        })
      ],
      sink: collectingSink(output),
      runOptions: { errorMode: "skip-and-collect" },
      state: skipState
    })

    expect(skipState.errors).toHaveLength(1)
    expect(output).toEqual([1, 3])
  })

  it("surfaces source errors and lets sink close be handled by the task lifecycle", async () => {
    const state = createRuntimeState()

    await expect(
      executePipeline({
        source: {
          kind: "source",
          name: "bad-source",
          async *read() {
            yield 1
            throw new Error("source failed")
          }
        },
        stages: [],
        sink: collectingSink([]),
        runOptions: {},
        state
      })
    ).rejects.toMatchObject({ stage: "bad-source#1" })
  })

  it("releases waiting work on cancel", async () => {
    const state = createRuntimeState()

    const run = executePipeline({
      source: {
        kind: "source",
        name: "slow-source",
        async *read() {
          yield 1
          await new Promise(() => undefined)
        }
      },
      stages: [],
      batchOptions: { size: 10, maxWaitMs: 1000 },
      sink: collectingSink([]),
      runOptions: {},
      state
    })

    await delay(5)
    state.ctx.abort("stop")
    await expect(run).rejects.toMatchObject({ code: "PIPELINE_ABORTED" })
  })
})

function source<T>(items: readonly T[]): Source<T> {
  return {
    kind: "source",
    name: "source",
    async *read() {
      yield* items
    }
  }
}

function collectingSink<T>(output: T[]): Sink<T> {
  return {
    kind: "sink",
    name: "sink",
    write(batch: Batch<T>) {
      output.push(...batch.items)
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
