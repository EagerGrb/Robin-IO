import { describe, expect, it } from "vitest"
import { createRuntimeChannel, type RuntimeChannel } from "./channel.js"
import { createRuntimeState } from "./runtime.js"
import { runBatchRunner, runDecoderRunner, runEncoderRunner, runSinkRunner, runTransformRunner } from "./runners.js"
import type { RuntimeState } from "./runtime.js"
import type { Decoder, Encoder, Transform } from "./types.js"

describe("TransformRunner", () => {
  it("runs a single-concurrency transform like applyTransform", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })

    await pushAll(input, state, [1, 2, 3])
    const run = runTransformRunner({
      stage: transform((value: number) => value * 2),
      stageId: "map#1",
      input,
      output,
      state
    })

    await run
    await expect(drain(output, state)).resolves.toEqual([2, 4, 6])
  })

  it("emits unordered concurrent results in completion order", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })

    await pushAll(input, state, [1, 2, 3])
    await runTransformRunner({
      stage: transform(
        async (value: number) => {
          await delay((4 - value) * 5)
          return value
        },
        { concurrency: 3, preserveOrder: false }
      ),
      stageId: "map#1",
      input,
      output,
      state
    })

    await expect(drain(output, state)).resolves.toEqual([3, 2, 1])
  })

  it("emits ordered concurrent results in input order", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })

    await pushAll(input, state, [1, 2, 3])
    await runTransformRunner({
      stage: transform(
        async (value: number) => {
          await delay((4 - value) * 5)
          return value
        },
        { concurrency: 3, preserveOrder: true }
      ),
      stageId: "map#1",
      input,
      output,
      state
    })

    await expect(drain(output, state)).resolves.toEqual([1, 2, 3])
  })

  it("limits active upstream pulls with maxInFlight", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })
    let started = 0
    let releaseHandlers: () => void = () => {}
    const handlersReleased = new Promise<void>((resolve) => {
      releaseHandlers = resolve
    })

    await pushAll(input, state, [1, 2, 3, 4])
    const run = runTransformRunner({
      stage: transform(
        async (value: number) => {
          started += 1
          await handlersReleased
          return value
        },
        { concurrency: 8, maxInFlight: 2 }
      ),
      stageId: "map#1",
      input,
      output,
      state
    })

    await waitFor(() => started >= 2)
    await delay(5)
    expect(started).toBe(2)

    releaseHandlers()
    await run
    await expect(drain(output, state)).resolves.toEqual([1, 2, 3, 4])
  })

  it("emits backpressure when the output channel is full", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 1 })

    await pushAll(input, state, [1, 2, 3])
    const run = runTransformRunner({
      stage: transform((value: number) => value),
      stageId: "map#1",
      input,
      output,
      state
    })

    await waitFor(() => output.size === 1)
    await delay(5)
    expect(state.ctx.metrics.snapshot()["channel.output.backpressure"]).toBeGreaterThanOrEqual(1)
    expect(await output.pull(state.ctx)).toEqual({ done: false, value: 1 })
    expect(await output.pull(state.ctx)).toEqual({ done: false, value: 2 })
    await run
    await expect(drain(output, state)).resolves.toEqual([3])
  })

  it("collects transform errors and continues in skip-and-collect mode", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })

    await pushAll(input, state, [1, 2, 3])
    await runTransformRunner({
      stage: transform((value: number) => {
        if (value === 2) {
          throw new Error("bad item")
        }
        return value
      }),
      stageId: "map#1",
      input,
      output,
      state,
      errorMode: "skip-and-collect"
    })

    expect(state.errors).toHaveLength(1)
    expect(state.errors[0]?.stage).toBe("map#1")
    await expect(drain(output, state)).resolves.toEqual([1, 3])
  })

  it("fails fast and fails the output channel on transform error", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })

    await pushAll(input, state, [1, 2, 3])
    await expect(
      runTransformRunner({
        stage: transform((value: number) => {
          if (value === 2) {
            throw new Error("bad item")
          }
          return value
        }),
        stageId: "map#1",
        input,
        output,
        state,
        errorMode: "fail-fast"
      })
    ).rejects.toMatchObject({ stage: "map#1" })

    expect(state.errors[0]).toMatchObject({
      stage: "map#1",
      input: 2,
      metadata: { stageName: "map" }
    })
    await expect(output.pull(state.ctx)).rejects.toMatchObject({ stage: "map#1" })
  })

  it("keeps only the first fail-fast error from concurrent handlers", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })

    await pushAll(input, state, [1, 2, 3])
    await expect(
      runTransformRunner({
        stage: transform(
          async (value: number) => {
            await delay(value)
            throw new Error(`bad item ${value}`)
          },
          { concurrency: 3 }
        ),
        stageId: "map#1",
        input,
        output,
        state,
        errorMode: "fail-fast"
      })
    ).rejects.toMatchObject({ stage: "map#1", input: 1 })

    await delay(10)
    expect(state.errors).toHaveLength(1)
    expect(state.errors[0]).toMatchObject({
      stage: "map#1",
      input: 1,
      message: "bad item 1"
    })
    await expect(output.pull(state.ctx)).rejects.toMatchObject({ stage: "map#1", input: 1 })
  })

  it("waits for already-started concurrent handlers before closing managed transforms", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })
    const events: string[] = []

    await pushAll(input, state, [1, 2])
    await expect(
      runTransformRunner({
        stage: {
          kind: "transform",
          name: "managed-concurrent",
          concurrency: 2,
          async handle(value: number) {
            events.push(`start:${value}`)
            if (value === 1) {
              await delay(1)
              events.push("throw:1")
              throw new Error("first failed")
            }
            await delay(10)
            events.push("finish:2")
            return value
          },
          close() {
            events.push("close")
          }
        },
        stageId: "managed-concurrent#1",
        input,
        output,
        state,
        errorMode: "fail-fast"
      })
    ).rejects.toMatchObject({ stage: "managed-concurrent#1", input: 1 })

    expect(events).toEqual(["start:1", "start:2", "throw:1", "finish:2", "close"])
    expect(state.errors).toHaveLength(1)
  })

  it("fails preserveOrder waiters when an earlier concurrent item fails", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })
    const events: string[] = []

    await pushAll(input, state, [1, 2])
    await expect(
      runTransformRunner({
        stage: transform(
          async (value: number) => {
            events.push(`start:${value}`)
            if (value === 1) {
              await delay(5)
              throw new Error("first failed")
            }
            events.push(`finish:${value}`)
            return value
          },
          { concurrency: 2, preserveOrder: true }
        ),
        stageId: "ordered#1",
        input,
        output,
        state,
        errorMode: "fail-fast"
      })
    ).rejects.toMatchObject({ stage: "ordered#1", input: 1 })

    await delay(10)
    expect(events).toEqual(["start:1", "start:2", "finish:2"])
    expect(state.errors).toHaveLength(1)
    await expect(output.pull(state.ctx)).rejects.toMatchObject({ stage: "ordered#1", input: 1 })
  })

  it("stops starting new handlers after abort", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })
    let started = 0

    await pushAll(input, state, [1, 2, 3])
    await expect(
      runTransformRunner({
        stage: transform((value: number, ctx) => {
          started += 1
          if (value === 1) {
            ctx.abort("stop")
          }
          return value
        }),
        stageId: "map#1",
        input,
        output,
        state
      })
    ).rejects.toMatchObject({ code: "PIPELINE_ABORTED" })

    expect(started).toBe(1)
  })

  it("opens and closes transform resources once, including failures", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "input", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "output", capacity: 8 })
    const events: string[] = []

    await pushAll(input, state, [1])
    await expect(
      runTransformRunner({
        stage: {
          kind: "transform",
          name: "managed",
          open() {
            events.push("open")
          },
          handle() {
            events.push("handle")
            throw new Error("boom")
          },
          close() {
            events.push("close")
          }
        },
        stageId: "managed#1",
        input,
        output,
        state
      })
    ).rejects.toMatchObject({ stage: "managed#1" })

    expect(events).toEqual(["open", "handle", "close"])
  })
})

describe("Codec runners", () => {
  it("decodes byte chunks through DecoderRunner", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<Uint8Array>({ name: "bytes", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "records", capacity: 8 })
    const encoder = new TextEncoder()

    await pushAll(input, state, [encoder.encode("1"), encoder.encode("2")])
    await runDecoderRunner({
      stage: decoder((chunk) => Number(new TextDecoder().decode(chunk))),
      stageId: "number-decoder#1",
      input,
      output,
      state
    })

    await expect(drain(output, state)).resolves.toEqual([1, 2])
    expect(state.ctx.metrics.snapshot()).toMatchObject({
      "stage.number-decoder#1.items": 2,
      "stage.number-decoder#1.handled": 2
    })
  })

  it("encodes records through EncoderRunner", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "records", capacity: 8 })
    const output = createRuntimeChannel<Uint8Array>({ name: "bytes", capacity: 8 })
    const textDecoder = new TextDecoder()

    await pushAll(input, state, [1, 2])
    await runEncoderRunner({
      stage: encoder((value) => new TextEncoder().encode(String(value))),
      stageId: "number-encoder#1",
      input,
      output,
      state
    })

    const chunks = await drain(output, state)
    expect(chunks.map((chunk) => textDecoder.decode(chunk))).toEqual(["1", "2"])
    expect(state.ctx.metrics.snapshot()).toMatchObject({
      "stage.number-encoder#1.items": 2,
      "stage.number-encoder#1.handled": 2
    })
  })

  it("wraps decoder errors with the codec stage id", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<Uint8Array>({ name: "bytes", capacity: 8 })
    const output = createRuntimeChannel<number>({ name: "records", capacity: 8 })

    await pushAll(input, state, [new TextEncoder().encode("bad")])
    await expect(
      runDecoderRunner({
        stage: decoder(() => {
          throw new Error("decode failed")
        }),
        stageId: "number-decoder#1",
        input,
        output,
        state
      })
    ).rejects.toMatchObject({ stage: "number-decoder#1" })
    expect(state.errors[0]).toMatchObject({
      stage: "number-decoder#1",
      metadata: { stageName: "number-decoder" }
    })
    await expect(output.pull(state.ctx)).rejects.toMatchObject({ stage: "number-decoder#1" })
  })
})

describe("BatchRunner", () => {
  it("batches items and emits flush metrics", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "items", capacity: 8 })
    const output = createRuntimeChannel({ name: "batches", capacity: 8 })

    await pushAll(input, state, [1, 2, 3])
    await runBatchRunner({ stageId: "batch#1", input, output, state, options: { size: 2 } })

    const batches = await drain(output, state)
    expect(batches.map((batch) => batch.items)).toEqual([[1, 2], [3]])
    expect(state.ctx.metrics.snapshot()).toMatchObject({
      "batch.batch#1.flushes": 2,
      "batch.batch#1.records": 3
    })
  })

  it("emits backpressure when the batch output channel is full", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel<number>({ name: "items", capacity: 8 })
    const output = createRuntimeChannel({ name: "batches", capacity: 1 })

    await pushAll(input, state, [1, 2, 3])
    const run = runBatchRunner({ stageId: "batch#1", input, output, state, options: { size: 1 } })

    await waitFor(() => output.size === 1)
    await delay(5)
    expect(state.ctx.metrics.snapshot()["channel.batches.backpressure"]).toBeGreaterThanOrEqual(1)
    expect((await output.pull(state.ctx)).done).toBe(false)
    expect((await output.pull(state.ctx)).done).toBe(false)
    await run
    expect((await output.pull(state.ctx)).done).toBe(false)
    expect((await output.pull(state.ctx)).done).toBe(true)
  })
})

describe("SinkRunner", () => {
  it("writes batches and emits sink write metrics", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel({ name: "batches", capacity: 8 })
    const written: number[] = []

    await input.push(createBatch([1, 2]), state.ctx)
    await input.push(createBatch([3]), state.ctx)
    input.close()
    await runSinkRunner({
      stageId: "sink#1",
      input,
      state,
      sink: {
        kind: "sink",
        name: "collector",
        write(batch) {
          written.push(...batch.items)
        }
      }
    })

    expect(written).toEqual([1, 2, 3])
    expect(state.ctx.metrics.snapshot()).toMatchObject({
      "sink.sink#1.writes": 2,
      "sink.sink#1.records": 3,
      "batches.written": 2,
      "records.written": 3
    })
  })

  it("fails fast when sink write fails", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel({ name: "batches", capacity: 8 })

    await input.push(createBatch([1]), state.ctx)
    input.close()
    await expect(
      runSinkRunner({
        stageId: "sink#1",
        input,
        state,
        sink: {
          kind: "sink",
          name: "failing-sink",
          write() {
            throw new Error("sink failed")
          }
        }
      })
    ).rejects.toMatchObject({ stage: "sink#1" })
    expect(state.errors[0]).toMatchObject({ stage: "sink#1" })
  })

  it("closes managed sinks after close errors surface", async () => {
    const state = createRuntimeState()
    const input = createRuntimeChannel({ name: "batches", capacity: 8 })
    const events: string[] = []

    input.close()
    await expect(
      runSinkRunner({
        stageId: "sink#1",
        input,
        state,
        sink: {
          kind: "sink",
          name: "managed",
          open() {
            events.push("open")
          },
          write() {},
          close() {
            events.push("close")
            throw new Error("close failed")
          }
        }
      })
    ).rejects.toThrow("close failed")
    expect(events).toEqual(["open", "close"])
  })
})

function transform<I, O>(
  handle: Transform<I, O>["handle"],
  options: Partial<Pick<Transform<I, O>, "concurrency" | "preserveOrder" | "maxInFlight">> = {}
): Transform<I, O> {
  return {
    kind: "transform",
    name: "map",
    ...options,
    handle
  }
}

function decoder<T>(parse: (chunk: Uint8Array) => T): Decoder<T> {
  return {
    kind: "decoder",
    name: "number-decoder",
    async *decode(input) {
      for await (const chunk of input) {
        yield parse(chunk)
      }
    }
  }
}

function encoder<T>(serialize: (input: T) => Uint8Array): Encoder<T> {
  return {
    kind: "encoder",
    name: "number-encoder",
    async *encode(input) {
      for await (const item of input) {
        yield serialize(item)
      }
    }
  }
}

async function pushAll<T>(channel: RuntimeChannel<T>, state: RuntimeState, items: readonly T[]): Promise<void> {
  for (const item of items) {
    await channel.push(item, state.ctx)
  }
  channel.close()
}

async function drain<T>(channel: RuntimeChannel<T>, state: RuntimeState): Promise<T[]> {
  const items: T[] = []
  while (true) {
    const result = await channel.pull(state.ctx)
    if (result.done) {
      return items
    }
    items.push(result.value)
  }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (predicate()) {
      return
    }
    await delay(1)
  }
  throw new Error("Timed out waiting for predicate")
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function createBatch<T>(items: readonly T[]) {
  return {
    id: 0,
    items,
    size: items.length,
    createdAt: Date.now()
  }
}
