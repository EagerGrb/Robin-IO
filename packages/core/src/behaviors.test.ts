import { describe, expect, it } from "vitest"
import { cancellationBehavior, deadLetterBehavior, map, pipeline, progressBehavior, validate } from "./index"
import { channelPush, stageItemEnd } from "./events.js"

describe("runtime behaviors", () => {
  it("flushes batches by maxWaitMs before size is reached", async () => {
    const batches: number[][] = []

    const result = await pipeline()
      .from({
        kind: "source",
        name: "slow-source",
        async *read() {
          yield 1
          await new Promise((resolve) => setTimeout(resolve, 30))
          yield 2
        }
      })
      .batch({ size: 10, maxWaitMs: 5 })
      .to({
        kind: "sink",
        name: "collector",
        write(batch) {
          batches.push([...batch.items])
        }
      })
      .run()

    expect(result.ok).toBe(true)
    expect(batches).toEqual([[1], [2]])
  })

  it("writes transform errors as serializable dead-letter records", async () => {
    const records: unknown[] = []
    const deadLetter = deadLetterBehavior({
      kind: "sink",
      name: "errors",
      write(batch) {
        records.push(...batch.items)
      }
    })

    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
          yield 0
          yield 2
        }
      })
      .through(validate((value: number) => value > 0, "positive only"))
      .to({
        kind: "sink",
        name: "noop",
        write() {}
      })
      .run({ behaviors: [deadLetter], errorMode: "skip-and-collect" })

    expect(result.ok).toBe(false)
    expect(deadLetter.getWrittenCount()).toBe(1)
    expect(records).toHaveLength(1)
    expect(JSON.parse(JSON.stringify(records[0]))).toMatchObject({
      code: "RUNTIME_ERROR",
      message: "positive only",
      stage: "validate#1",
      payload: {
        runtime: "unknown"
      }
    })
  })

  it("can abort after an error threshold", async () => {
    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
          yield 2
          yield 3
        }
      })
      .through(
        map(() => {
          throw new Error("boom")
        })
      )
      .to({
        kind: "sink",
        name: "noop",
        write() {}
      })
      .run({ behaviors: [cancellationBehavior({ maxErrors: 1 })], errorMode: "skip-and-collect" })

    expect(result.ok).toBe(false)
    expect(result.errors.length).toBeGreaterThanOrEqual(1)
    expect(result.errors.some((error) => error.message.includes("aborted"))).toBe(true)
  })

  it("does not double count progress for new internal events", async () => {
    const progress = progressBehavior()

    await progress.onEvent?.(stageItemEnd({ stage: "map#1", durationMs: 1 }), {} as never)
    await progress.onEvent?.(channelPush({ channel: "a-b", size: 1, capacity: 2 }), {} as never)

    expect(progress.getSnapshot()).toEqual({
      recordsRead: 0,
      recordsHandled: 0,
      recordsSkipped: 0,
      recordsWritten: 0,
      batchesWritten: 0,
      errors: 0
    })
  })

  it("delivers internal events to behavior onEvent hooks", async () => {
    const eventTypes: string[] = []

    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
        }
      })
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run({
        behaviors: [
          {
            name: "event-recorder",
            onEvent(event) {
              eventTypes.push(event.type)
            }
          }
        ]
      })

    expect(result.ok).toBe(true)
    expect(eventTypes).toEqual(
      expect.arrayContaining(["channel.push", "channel.pull", "batch.flush.end", "sink.write.end"])
    )
  })

  it("fails fast when a behavior throws", async () => {
    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
        }
      })
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run({
        behaviors: [
          {
            name: "throwing-behavior",
            onBatch() {
              throw new Error("behavior failed")
            }
          }
        ]
      })

    expect(result.ok).toBe(false)
    expect(result.errors.some((error) => error.message === "behavior failed")).toBe(true)
  })
})
