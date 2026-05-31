import { describe, expect, it } from "vitest"
import { createRuntimeChannel } from "./channel.js"
import { createRuntimeState } from "./runtime.js"
import { RuntimeError } from "./types.js"

describe("RuntimeChannel", () => {
  it("waits when capacity is full and resumes after pull", async () => {
    const state = createRuntimeState()
    const channel = createRuntimeChannel<number>({ name: "items", capacity: 1 })

    await channel.push(1, state.ctx)
    let secondPushResolved = false
    const secondPush = channel.push(2, state.ctx).then(() => {
      secondPushResolved = true
    })

    await Promise.resolve()
    expect(secondPushResolved).toBe(false)
    await expect(channel.pull(state.ctx)).resolves.toEqual({ done: false, value: 1 })
    await secondPush
    expect(secondPushResolved).toBe(true)
    await expect(channel.pull(state.ctx)).resolves.toEqual({ done: false, value: 2 })
    expect(state.ctx.metrics.snapshot()).toMatchObject({
      "channel.items.wait": 1,
      "channel.items.wait.push": 1
    })
  })

  it("waits when empty and resumes after push", async () => {
    const state = createRuntimeState()
    const channel = createRuntimeChannel<number>({ name: "items", capacity: 1 })

    const pull = channel.pull(state.ctx)
    await Promise.resolve()
    await channel.push(1, state.ctx)

    await expect(pull).resolves.toEqual({ done: false, value: 1 })
  })

  it("drains queued items after close and then returns done", async () => {
    const state = createRuntimeState()
    const channel = createRuntimeChannel<number>({ name: "items", capacity: 2 })

    await channel.push(1, state.ctx)
    await channel.push(2, state.ctx)
    channel.close()

    await expect(channel.pull(state.ctx)).resolves.toEqual({ done: false, value: 1 })
    await expect(channel.pull(state.ctx)).resolves.toEqual({ done: false, value: 2 })
    await expect(channel.pull(state.ctx)).resolves.toEqual({ done: true, value: undefined })
  })

  it("fails waiting and future push and pull operations with the same error", async () => {
    const state = createRuntimeState()
    const channel = createRuntimeChannel<number>({ name: "items", capacity: 1 })
    const error = new RuntimeError("boom", { code: "BOOM" })

    const waitingPull = channel.pull(state.ctx)
    channel.fail(error)

    await expect(waitingPull).rejects.toBe(error)
    await expect(channel.push(1, state.ctx)).rejects.toBe(error)
    await expect(channel.pull(state.ctx)).rejects.toBe(error)
  })

  it("releases a waiting pull after abort", async () => {
    const controller = new AbortController()
    const state = createRuntimeState({ signal: controller.signal })
    const channel = createRuntimeChannel<number>({ name: "items", capacity: 1 })

    const waitingPull = channel.pull(state.ctx)
    controller.abort("stop")

    await expect(waitingPull).rejects.toMatchObject({ code: "PIPELINE_ABORTED" })
  })

  it("releases a waiting push after abort", async () => {
    const controller = new AbortController()
    const state = createRuntimeState({ signal: controller.signal })
    const channel = createRuntimeChannel<number>({ name: "items", capacity: 1 })

    await channel.push(1, state.ctx)
    const waitingPush = channel.push(2, state.ctx)
    controller.abort("stop")

    await expect(waitingPush).rejects.toMatchObject({ code: "PIPELINE_ABORTED" })
  })
})
