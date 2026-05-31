import { webWorkerTransform } from "@robbin-io/browser"
import { pipeline } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"
import { memorySource } from "@robbin-io/source-memory"

interface BenchCaseResult {
  readonly name: string
  readonly meanMs: number
  readonly samples: number[]
  readonly totalBytes: number
}

interface BenchResult {
  readonly ok: boolean
  readonly payloadCount: number
  readonly payloadSize: number
  readonly iterations: number
  readonly results: BenchCaseResult[]
  readonly errors: string[]
}

const output = document.querySelector<HTMLPreElement>("#result")
const params = new URLSearchParams(location.search)
const payloadCount = Number(params.get("count") ?? 10)
const payloadSize = Number(params.get("size") ?? 64 * 1024)
const iterations = Number(params.get("iterations") ?? 2)

void runBenchmark()
  .then((result) => writeResult(result))
  .catch((error: unknown) =>
    writeResult({
      ok: false,
      payloadCount,
      payloadSize,
      iterations,
      results: [],
      errors: [error instanceof Error ? error.message : String(error)]
    })
  )

async function runBenchmark(): Promise<BenchResult> {
  writeStatus("running clone")
  const cloneResult = await runCase("clone", false)
  writeStatus("running transfer")
  const transferResult = await runCase("transfer", true)
  const results = [cloneResult, transferResult]
  const expectedBytes = payloadCount * payloadSize
  const ok = results.every((result) => result.totalBytes === expectedBytes && result.samples.length === iterations)

  return {
    ok,
    payloadCount,
    payloadSize,
    iterations,
    results,
    errors: []
  }
}

async function runCase(name: string, transfer: boolean): Promise<BenchCaseResult> {
  const samples: number[] = []
  let totalBytes = 0

  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const payloads = createPayloads()
    const sink = memorySink<number>()
    const transform = webWorkerTransform<ArrayBuffer, number>(
      () => new Worker(new URL("./worker.ts", import.meta.url), { type: "module" }),
      {
        name: `browser-worker-bench-${name}`,
        concurrency: 8,
        maxPending: 16,
        timeoutMs: 30_000,
        transfer: transfer ? (input) => [input] : undefined
      }
    )

    const startedAt = performance.now()
    const result = await pipeline()
      .from(memorySource(payloads))
      .through(transform)
      .batch({ size: 100 })
      .to(sink)
      .run({ runtime: "browser" })
    const durationMs = performance.now() - startedAt

    totalBytes = sum(sink.getItems())
    const detached = transfer ? payloads.every((payload) => payload.byteLength === 0) : true
    if (!result.ok || result.errors.length > 0 || totalBytes !== payloadCount * payloadSize || !detached) {
      throw new Error(
        `${name} browser worker benchmark failed: ${JSON.stringify({
          ok: result.ok,
          totalBytes,
          detached,
          errors: result.errors.map((error) => `${error.code}:${error.message}`)
        })}`
      )
    }
    samples.push(durationMs)
  }

  return {
    name,
    meanMs: mean(samples),
    samples,
    totalBytes
  }
}

function createPayloads(): ArrayBuffer[] {
  return Array.from({ length: payloadCount }, () => new ArrayBuffer(payloadSize))
}

function mean(values: number[]): number {
  return sum(values) / values.length
}

function sum(values: readonly number[]): number {
  return values.reduce((total, value) => total + value, 0)
}

function writeResult(result: BenchResult): void {
  if (output) {
    output.textContent = JSON.stringify(result)
  }
  ;(globalThis as typeof globalThis & { __IO_BROWSER_WORKER_BENCH__?: BenchResult }).__IO_BROWSER_WORKER_BENCH__ =
    result
}

function writeStatus(status: string): void {
  if (output) {
    output.textContent = JSON.stringify({ ok: false, status, payloadCount, payloadSize, iterations })
  }
}
