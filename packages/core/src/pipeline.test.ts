import { describe, expect, it } from "vitest"
import fc from "fast-check"
import {
  filter,
  map,
  pipeline,
  progressBehavior,
  validate,
  type Batch,
  type PipelinePlugin,
  type RuntimeBehavior,
  type RuntimeEvent
} from "./index"

describe("pipeline", () => {
  it("runs a linear AsyncIterable pipeline with batching", async () => {
    const output: number[][] = []
    const progress = progressBehavior()

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
      .through(map((value: number) => value * 2))
      .through(filter((value: number) => value > 2))
      .batch({ size: 2 })
      .to({
        kind: "sink",
        name: "collector",
        write(batch) {
          output.push([...batch.items])
        }
      })
      .run({ behaviors: [progress] })

    expect(result.ok).toBe(true)
    expect(output).toEqual([[4, 6]])
    expect(progress.getSnapshot().recordsRead).toBe(3)
    expect(progress.getSnapshot().batchesWritten).toBe(1)
    expect(result.metrics["channel.numbers#1:source.push"]).toBeUndefined()
    expect(result.metrics["channel.map#1:input.push"]).toBeUndefined()
    expect(result.metrics["channel.filter#1:input.push"]).toBeUndefined()
  })

  it("can skip transform failures when configured", async () => {
    const output: number[] = []

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
        name: "collector",
        write(batch) {
          output.push(...batch.items)
        }
      })
      .run({ errorMode: "skip-and-collect" })

    expect(result.ok).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(output).toEqual([1, 2])
  })

  it("supports pipeline plugins without binding the core to a data format", async () => {
    const output: Array<{ id: string }> = []
    const normalizeId: PipelinePlugin<{ id: number }, { id: string }> = {
      name: "normalize-id",
      configure(builder) {
        return builder.through(map((row) => ({ id: String(row.id) })))
      }
    }

    const result = await pipeline()
      .from({
        kind: "source",
        name: "objects",
        async *read() {
          yield { id: 1 }
        }
      })
      .use(normalizeId)
      .to({
        kind: "sink",
        name: "collector",
        write(batch) {
          output.push(...batch.items)
        }
      })
      .run()

    expect(result.ok).toBe(true)
    expect(output).toEqual([{ id: "1" }])
  })

  it("uses stable stage ids for repeated stage names in metrics", async () => {
    const sinkItems: number[] = []

    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
        }
      })
      .through(map((value: number) => value + 1, { name: "map" }))
      .through(map((value: number) => value * 2, { name: "map" }))
      .to({
        kind: "sink",
        name: "collector",
        write(batch) {
          sinkItems.push(...batch.items)
        }
      })
      .run()

    expect(result.ok).toBe(true)
    expect(sinkItems).toEqual([4])
    expect(result.metrics).toMatchObject({
      "stage.map#1.handled": 1,
      "stage.map#2.handled": 1,
      "stage.map#1.items": 1,
      "stage.map#2.items": 1
    })
    expect(result.metrics["stage.map.handled"]).toBeUndefined()
  })

  it("uses the unobserved direct transform path for simple transforms", async () => {
    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
          yield 2
        }
      })
      .through(map((value: number) => value + 1))
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run()

    expect(result.ok).toBe(true)
    expect(result.metrics).toMatchObject({
      "stage.map#1.handled": 2,
      "stage.map#1.items": 2
    })
    expect(result.metrics["channel.map#1:input.push"]).toBeUndefined()
    expect(result.metrics["channel.map#1:output.pull"]).toBeUndefined()
  })

  it("keeps the channel-backed transform path when full event observation is enabled", async () => {
    const events: RuntimeEvent[] = []

    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
        }
      })
      .through(map((value: number) => value + 1))
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run({ onEvent: (event) => events.push(event) })

    expect(result.ok).toBe(true)
    expect(
      events.some(
        (event) =>
          (event.type === "channel.push" || event.type === "channel.pull") && event.channel.startsWith("map#1:")
      )
    ).toBe(true)
    expect(events.some((event) => event.type === "stage.item.start" && event.stage === "map#1")).toBe(true)
  })

  it("uses the unobserved fused batch/sink path for simple output", async () => {
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
      .batch({ size: 2 })
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run()

    expect(result.ok).toBe(true)
    expect(result.metrics).toMatchObject({
      "batch.batch#1.flushes": 2,
      "batch.batch#1.records": 3,
      "sink.collector#1.writes": 2,
      "records.written": 3
    })
    expect(result.metrics["channel.pipeline:items.push"]).toBeUndefined()
    expect(result.metrics["channel.pipeline:batches.pull"]).toBeUndefined()
  })

  it("uses the unobserved direct source path when full event observation is absent", async () => {
    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
          yield 2
        }
      })
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run()

    expect(result.ok).toBe(true)
    expect(result.metrics).toMatchObject({
      "records.read": 2,
      "stage.numbers#1.read": 2
    })
    expect(result.metrics["channel.numbers#1:source.push"]).toBeUndefined()
    expect(result.metrics["channel.numbers#1:source.pull"]).toBeUndefined()
  })

  it("keeps the channel-backed batch/sink path when full event observation is enabled", async () => {
    const events: RuntimeEvent[] = []

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
      .run({ onEvent: (event) => events.push(event) })

    expect(result.ok).toBe(true)
    expect(
      events.some(
        (event) =>
          (event.type === "channel.push" || event.type === "channel.pull") &&
          (event.channel === "pipeline:items" || event.channel === "pipeline:batches")
      )
    ).toBe(true)
  })

  it("keeps the channel-backed source path when full event observation is enabled", async () => {
    const events: RuntimeEvent[] = []

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
      .run({ onEvent: (event) => events.push(event) })

    expect(result.ok).toBe(true)
    expect(
      events.some(
        (event) =>
          (event.type === "channel.push" || event.type === "channel.pull") && event.channel === "numbers#1:source"
      )
    ).toBe(true)
  })

  it("emits format-agnostic runtime events", async () => {
    const events: RuntimeEvent[] = []

    await pipeline()
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
      .run({ onEvent: (event) => events.push(event) })

    expect(events.map((event) => event.type)).toEqual(
      expect.arrayContaining([
        "start",
        "channel.push",
        "channel.pull",
        "batch.flush.end",
        "sink.write.end",
        "record",
        "batch",
        "finish"
      ])
    )
    expect(events.every((event) => typeof event.timestamp === "number")).toBe(true)
  })

  it("opens and closes stage resources", async () => {
    const events: string[] = []

    await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
        }
      })
      .through({
        kind: "transform",
        name: "managed",
        open() {
          events.push("open")
        },
        handle(value: number) {
          events.push("handle")
          return value
        },
        close() {
          events.push("close")
        }
      })
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run()

    expect(events).toEqual(["open", "handle", "close"])
  })

  it("exposes task status and prevents repeated run", async () => {
    const task = pipeline()
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

    expect(task.status).toBe("idle")
    const result = await task.run()
    expect(result.ok).toBe(true)
    expect(task.status).toBe("finished")
    await expect(task.run()).rejects.toMatchObject({ code: "PIPELINE_ALREADY_RUN" })
  })

  it("can be cancelled through PipelineTask.cancel", async () => {
    const events: string[] = []
    const task = pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
          await delay(1000)
          yield 2
        },
        close() {
          events.push("source.close")
        }
      })
      .to({
        kind: "sink",
        name: "collector",
        async write() {
          task.cancel("stop")
        },
        close() {
          events.push("sink.close")
        }
      })

    const result = await task.run()
    expect(result.ok).toBe(false)
    expect(result.errors.some((error) => error.code === "PIPELINE_ABORTED")).toBe(true)
    expect(task.status).toBe("failed")
    expect(events).toEqual(["sink.close", "source.close"])
  })

  it("closes already-open resources when stage open fails", async () => {
    const events: string[] = []

    const result = await pipeline()
      .from({
        kind: "source",
        name: "source",
        open() {
          events.push("source.open")
        },
        async *read() {},
        close() {
          events.push("source.close")
        }
      })
      .through({
        kind: "transform",
        name: "first",
        open() {
          events.push("first.open")
        },
        handle(value: unknown) {
          return value
        },
        close() {
          events.push("first.close")
        }
      })
      .through({
        kind: "transform",
        name: "second",
        open() {
          events.push("second.open")
          throw new Error("open failed")
        },
        handle(value: unknown) {
          return value
        },
        close() {
          events.push("second.close")
        }
      })
      .to({
        kind: "sink",
        name: "sink",
        open() {
          events.push("sink.open")
        },
        write() {},
        close() {
          events.push("sink.close")
        }
      })
      .run()

    expect(result.ok).toBe(false)
    expect(result.errors[0]?.message).toBe("open failed")
    expect(events).toEqual(["source.open", "first.open", "second.open", "first.close", "source.close"])
  })

  it("does not close source when source open fails", async () => {
    const events: string[] = []

    const result = await pipeline()
      .from({
        kind: "source",
        name: "source",
        open() {
          events.push("source.open")
          throw new Error("source open failed")
        },
        async *read() {},
        close() {
          events.push("source.close")
        }
      })
      .to({
        kind: "sink",
        name: "sink",
        write() {},
        close() {
          events.push("sink.close")
        }
      })
      .run()

    expect(result.ok).toBe(false)
    expect(result.errors[0]?.message).toBe("source open failed")
    expect(events).toEqual(["source.open"])
  })

  it("collects all close errors", async () => {
    const result = await pipeline()
      .from({
        kind: "source",
        name: "source",
        async *read() {
          yield 1
        },
        close() {
          throw new Error("source close failed")
        }
      })
      .through({
        kind: "transform",
        name: "transform",
        handle(value: number) {
          return value
        },
        close() {
          throw new Error("transform close failed")
        }
      })
      .to({
        kind: "sink",
        name: "sink",
        write() {},
        close() {
          throw new Error("sink close failed")
        }
      })
      .run()

    expect(result.ok).toBe(false)
    expect(result.errors.map((error) => error.message)).toEqual([
      "sink close failed",
      "transform close failed",
      "source close failed"
    ])
  })

  it("emits finish only once when cancellation and close errors overlap", async () => {
    const events: RuntimeEvent[] = []

    await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          yield 1
        },
        close() {
          throw new Error("close failed")
        }
      })
      .through(
        map((value: number, ctx) => {
          ctx.abort("stop")
          return value
        })
      )
      .to({
        kind: "sink",
        name: "sink",
        write() {}
      })
      .run({ onEvent: (event) => events.push(event) })

    expect(events.filter((event) => event.type === "finish")).toHaveLength(1)
  })

  it("closes source, transform, and sink after mid-pipeline abort", async () => {
    const events: string[] = []
    const controller = new AbortController()

    const result = await pipeline()
      .from({
        kind: "source",
        name: "numbers",
        open() {
          events.push("source.open")
        },
        async *read() {
          yield 1
          yield 2
          yield 3
        },
        close() {
          events.push("source.close")
        }
      })
      .through({
        kind: "transform",
        name: "abort-after-first",
        open() {
          events.push("transform.open")
        },
        handle(value: number) {
          if (value === 2) {
            controller.abort("baseline abort")
          }
          return value
        },
        close() {
          events.push("transform.close")
        }
      })
      .to({
        kind: "sink",
        name: "collector",
        open() {
          events.push("sink.open")
        },
        write() {},
        close() {
          events.push("sink.close")
        }
      })
      .run({ signal: controller.signal })

    expect(result.ok).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0]?.code).toBe("PIPELINE_ABORTED")
    expect(events).toEqual([
      "source.open",
      "transform.open",
      "sink.open",
      "sink.close",
      "transform.close",
      "source.close"
    ])
  })

  it.each([
    {
      name: "source read failure",
      failAt: "source" as const,
      expectedMessage: "source read failed"
    },
    {
      name: "transform handler failure",
      failAt: "transform" as const,
      expectedMessage: "transform failed"
    },
    {
      name: "sink write failure",
      failAt: "sink" as const,
      expectedMessage: "sink write failed"
    },
    {
      name: "external abort",
      failAt: "abort" as const,
      expectedMessage: "Pipeline aborted"
    }
  ])("closes opened resources after $name", async ({ failAt, expectedMessage }) => {
    const events: string[] = []
    const controller = new AbortController()

    const result = await pipeline()
      .from({
        kind: "source",
        name: "fault-source",
        open() {
          events.push("source.open")
        },
        async *read() {
          events.push("source.read.1")
          yield 1
          if (failAt === "source") {
            events.push("source.throw")
            throw new Error(expectedMessage)
          }
          events.push("source.read.2")
          yield 2
        },
        close() {
          events.push("source.close")
        }
      })
      .through({
        kind: "transform",
        name: "fault-transform",
        open() {
          events.push("transform.open")
        },
        handle(value: number) {
          events.push(`transform.handle.${value}`)
          if (failAt === "transform" && value === 1) {
            throw new Error(expectedMessage)
          }
          if (failAt === "abort" && value === 1) {
            controller.abort("fault injection")
          }
          return value
        },
        close() {
          events.push("transform.close")
        }
      })
      .to({
        kind: "sink",
        name: "fault-sink",
        open() {
          events.push("sink.open")
        },
        write(batch: Batch<number>) {
          events.push(`sink.write.${batch.items.join(",")}`)
          if (failAt === "sink") {
            throw new Error(expectedMessage)
          }
        },
        close() {
          events.push("sink.close")
        }
      })
      .run({ signal: controller.signal })

    expect(result.ok).toBe(false)
    expect(result.errors.some((error) => error.message === expectedMessage)).toBe(true)
    expect(events).toEqual(expect.arrayContaining(["sink.close", "transform.close", "source.close"]))
    expect(events.filter((event) => event.endsWith(".close"))).toEqual([
      "sink.close",
      "transform.close",
      "source.close"
    ])
  })

  it("fails and releases resources when cancellation is injected at random pipeline points", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("read", "handled", "batch"),
        fc.integer({ min: 1, max: 4 }),
        async (mode, cancelAt) => {
          const events: string[] = []
          const output: number[] = []
          const behavior = cancellationAtBehavior(mode, cancelAt)

          const result = await pipeline()
            .from({
              kind: "source",
              name: "random-cancel-source",
              open() {
                events.push("source.open")
              },
              async *read() {
                for (let value = 0; value < 20; value += 1) {
                  yield value
                }
              },
              close() {
                events.push("source.close")
              }
            })
            .through({
              kind: "transform",
              name: "random-cancel-transform",
              open() {
                events.push("transform.open")
              },
              handle(value: number) {
                return value
              },
              close() {
                events.push("transform.close")
              }
            })
            .batch({ size: 2 })
            .to({
              kind: "sink",
              name: "random-cancel-sink",
              open() {
                events.push("sink.open")
              },
              write(batch: Batch<number>) {
                output.push(...batch.items)
              },
              close() {
                events.push("sink.close")
              }
            })
            .run({ behaviors: [behavior] })

          expect(result.ok).toBe(false)
          expect(result.errors.some((error) => error.code === "PIPELINE_ABORTED")).toBe(true)
          expect(events.filter((event) => event.endsWith(".close"))).toEqual([
            "sink.close",
            "transform.close",
            "source.close"
          ])
          expect(output.length).toBeLessThan(20)
        }
      ),
      { numRuns: 24 }
    )
  })

  it("documents that concurrent transforms currently emit completion order", async () => {
    const output: number[] = []

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
        map(
          async (value: number) => {
            await delay((4 - value) * 25)
            return value
          },
          { concurrency: 3 }
        )
      )
      .to({
        kind: "sink",
        name: "collector",
        write(batch: Batch<number>) {
          output.push(...batch.items)
        }
      })
      .run()

    expect(result.ok).toBe(true)
    expect(output).toEqual([3, 2, 1])
  })

  it("keeps the current transform fail-fast behavior", async () => {
    const output: number[] = []

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
        map((value: number) => {
          if (value === 2) {
            throw new Error("bad transform")
          }
          return value
        })
      )
      .to({
        kind: "sink",
        name: "collector",
        write(batch: Batch<number>) {
          output.push(...batch.items)
        }
      })
      .run({ errorMode: "fail-fast" })

    expect(result.ok).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0]?.stage).toBe("map#1")
    expect(output).toEqual([1])
  })

  it("keeps the current transform skip-and-collect behavior", async () => {
    const output: number[] = []

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
        map((value: number) => {
          if (value === 2) {
            throw new Error("bad transform")
          }
          return value
        })
      )
      .to({
        kind: "sink",
        name: "collector",
        write(batch: Batch<number>) {
          output.push(...batch.items)
        }
      })
      .run({ errorMode: "skip-and-collect" })

    expect(result.ok).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0]?.stage).toBe("map#1")
    expect(output).toEqual([1, 3])
  })

  it("keeps the current sink write error behavior", async () => {
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
        name: "failing-sink",
        write() {
          throw new Error("sink failed")
        }
      })
      .run({ errorMode: "skip-and-collect" })

    expect(result.ok).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0]?.stage).toBe("failing-sink#1")
  })

  it("keeps the current source error behavior", async () => {
    const result = await pipeline()
      .from({
        kind: "source",
        name: "failing-source",
        async *read() {
          yield 1
          throw new Error("source failed")
        }
      })
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run({ errorMode: "skip-and-collect" })

    expect(result.ok).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0]?.message).toBe("source failed")
  })

  it("collects sink close errors in the run result", async () => {
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
        name: "closing-sink",
        write() {},
        close() {
          throw new Error("close failed")
        }
      })
      .run()

    expect(result.ok).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0]).toMatchObject({
      stage: "closing-sink",
      message: "close failed"
    })
  })

  it("does not read unboundedly when sink is slow", async () => {
    let readCount = 0
    let firstWriteStarted: (() => void) | undefined
    const firstWrite = new Promise<void>((resolve) => {
      firstWriteStarted = resolve
    })
    let releaseSink: (() => void) | undefined
    const sinkReleased = new Promise<void>((resolve) => {
      releaseSink = resolve
    })

    const run = pipeline()
      .from({
        kind: "source",
        name: "numbers",
        async *read() {
          for (let value = 0; value < 100; value += 1) {
            readCount += 1
            yield value
          }
        }
      })
      .batch({ size: 1 })
      .to({
        kind: "sink",
        name: "slow-sink",
        async write() {
          firstWriteStarted?.()
          await sinkReleased
        }
      })
      .run({ channelCapacity: 4 })

    await firstWrite
    await delay(10)
    expect(readCount).toBeLessThan(20)
    releaseSink?.()
    const result = await run
    expect(result.ok).toBe(true)
    expect(readCount).toBe(100)
  })
})

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function cancellationAtBehavior(mode: "read" | "handled" | "batch", cancelAt: number): RuntimeBehavior {
  let seen = 0

  return {
    name: "random-cancellation",
    onRecord(event, ctx) {
      if (event.action !== mode) {
        return
      }
      seen += event.count
      if (seen >= cancelAt) {
        ctx.abort(`abort:${mode}:${cancelAt}`)
      }
    },
    onBatch(_event, ctx) {
      if (mode !== "batch") {
        return
      }
      seen += 1
      if (seen >= cancelAt) {
        ctx.abort(`abort:${mode}:${cancelAt}`)
      }
    }
  }
}
