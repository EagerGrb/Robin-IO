import { webWorkerTransform } from "@robbin-io/browser"
import { pipeline } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"
import { memorySource } from "@robbin-io/source-memory"

interface SmokeResult {
  readonly ok: boolean
  readonly received: number[]
  readonly detached: boolean[]
  readonly errors: string[]
}

const output = document.querySelector<HTMLPreElement>("#result")

void runSmoke()
  .then((result) => writeResult(result))
  .catch((error: unknown) =>
    writeResult({
      ok: false,
      received: [],
      detached: [],
      errors: [error instanceof Error ? error.message : String(error)]
    })
  )

async function runSmoke(): Promise<SmokeResult> {
  const payloads = [new ArrayBuffer(1024), new ArrayBuffer(2048), new ArrayBuffer(4096)]
  const sink = memorySink<number>()
  const workerTransform = webWorkerTransform<ArrayBuffer, number>(
    () => new Worker(new URL("./worker.ts", import.meta.url), { type: "module" }),
    {
      name: "browser-worker-smoke",
      concurrency: 2,
      maxPending: 2,
      timeoutMs: 5000,
      transfer(input) {
        return [input]
      }
    }
  )

  const result = await pipeline()
    .from(memorySource(payloads))
    .through(workerTransform)
    .batch({ size: 2 })
    .to(sink)
    .run({ runtime: "browser" })

  const received = sink.getItems()
  const detached = payloads.map((payload) => payload.byteLength === 0)
  const ok =
    result.ok &&
    result.errors.length === 0 &&
    JSON.stringify(received) === JSON.stringify([1024, 2048, 4096]) &&
    detached.every(Boolean)

  return {
    ok,
    received,
    detached,
    errors: result.errors.map((error) => `${error.code}:${error.message}`)
  }
}

function writeResult(result: SmokeResult): void {
  if (output) {
    output.textContent = JSON.stringify(result)
  }
  ;(globalThis as typeof globalThis & { __IO_BROWSER_WORKER_SMOKE__?: SmokeResult }).__IO_BROWSER_WORKER_SMOKE__ =
    result
}
