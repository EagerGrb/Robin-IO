import { describe, expect, it } from "vitest"
import { batchIterable } from "./batch.js"
import { createRuntimeState } from "./runtime.js"

describe("batchIterable", () => {
  it("emits an oversized single item when it exceeds maxBytes", async () => {
    const state = createRuntimeState()
    const batches = await collect(
      batchIterable(items(["large", "small"]), { maxBytes: 4, estimateBytes: (item) => item.length }, state.ctx)
    )

    expect(batches.map((batch) => batch.items)).toEqual([["large"], ["small"]])
    expect(batches.map((batch) => batch.bytes)).toEqual([5, 5])
  })

  it("flushes repeatedly by maxWaitMs", async () => {
    const state = createRuntimeState()
    const batches = await collect(batchIterable(slowItems(), { size: 10, maxWaitMs: 5 }, state.ctx))

    expect(batches.map((batch) => batch.items)).toEqual([[1], [2]])
  })

  it("releases maxWaitMs timers on abort", async () => {
    const controller = new AbortController()
    const state = createRuntimeState({ signal: controller.signal })
    const iterator = batchIterable(neverCompletesAfterFirst(), { size: 10, maxWaitMs: 1000 }, state.ctx)[
      Symbol.asyncIterator
    ]()

    const next = iterator.next()
    await delay(5)
    controller.abort("stop")

    await expect(next).rejects.toMatchObject({ code: "PIPELINE_ABORTED" })
  })
})

async function* slowItems(): AsyncIterable<number> {
  yield 1
  await delay(20)
  yield 2
}

async function* neverCompletesAfterFirst(): AsyncIterable<number> {
  yield 1
  await new Promise(() => undefined)
}

async function* items<T>(values: readonly T[]): AsyncIterable<T> {
  yield* values
}

async function collect<T>(input: AsyncIterable<T>): Promise<T[]> {
  const items: T[] = []
  for await (const item of input) {
    items.push(item)
  }
  return items
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
