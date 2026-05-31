import { CORE_RUNTIME_ERROR_CODES, RuntimeError } from "./types.js"
import type { PipelineStatus } from "./types.js"

const transitions: Readonly<Record<PipelineStatus, readonly PipelineStatus[]>> = {
  idle: ["opening"],
  opening: ["running", "closing", "failed", "aborting"],
  running: ["aborting", "closing", "failed"],
  aborting: ["closing", "failed"],
  closing: ["finished", "failed"],
  finished: [],
  failed: []
}

export class PipelineLifecycle {
  private current: PipelineStatus = "idle"
  private finishStarted = false

  get status(): PipelineStatus {
    return this.current
  }

  transition(next: PipelineStatus): void {
    if (this.current === next) {
      return
    }

    if (!transitions[this.current].includes(next)) {
      throw new RuntimeError(`Invalid pipeline status transition from ${this.current} to ${next}`, {
        code: CORE_RUNTIME_ERROR_CODES.invalidPipelineStatus,
        metadata: { from: this.current, to: next }
      })
    }

    this.current = next
  }

  beginFinish(): boolean {
    if (this.finishStarted) {
      return false
    }
    this.finishStarted = true
    return true
  }

  markTerminal(failed: boolean): void {
    this.transition(failed ? "failed" : "finished")
  }
}

export function createPipelineLifecycle(): PipelineLifecycle {
  return new PipelineLifecycle()
}
