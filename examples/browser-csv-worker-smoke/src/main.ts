import { blobSource, webWorkerTransform } from "@robbin-io/browser"
import { csvDecoder } from "@robbin-io/codec-csv"
import { pipeline, progressBehavior } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"

interface CsvRow {
  readonly id: string
  readonly name: string
  readonly amount: string
  readonly note: string
}

interface NormalizedRow {
  readonly id: number
  readonly name: string
  readonly amountCents: number
  readonly noteLength: number
}

interface BrowserCsvWorkerSmokeResult {
  readonly ok: boolean
  readonly rows: number
  readonly first?: NormalizedRow
  readonly last?: NormalizedRow
  readonly sumAmountCents: number
  readonly progress: {
    readonly recordsRead: number
    readonly recordsHandled: number
    readonly recordsWritten: number
  }
  readonly hasCsvChannelMetrics: boolean
  readonly errors: string[]
}

const output = document.querySelector<HTMLPreElement>("#result")

void runSmoke()
  .then((result) => writeResult(result))
  .catch((error: unknown) =>
    writeResult({
      ok: false,
      rows: 0,
      sumAmountCents: 0,
      progress: {
        recordsRead: 0,
        recordsHandled: 0,
        recordsWritten: 0
      },
      hasCsvChannelMetrics: false,
      errors: [error instanceof Error ? error.message : String(error)]
    })
  )

async function runSmoke(): Promise<BrowserCsvWorkerSmokeResult> {
  const rowCount = 3000
  const csv = createCsv(rowCount)
  const source = blobSource(new Blob([csv], { type: "text/csv" }), {
    name: "browser-csv-worker-blob",
    chunkSize: 769
  })
  const workerTransform = webWorkerTransform<CsvRow, NormalizedRow>(
    () => new Worker(new URL("./normalize.worker.ts", import.meta.url), { type: "module" }),
    {
      name: "browser-csv-worker-normalize",
      concurrency: 4,
      maxPending: 8,
      timeoutMs: 5000
    }
  )
  const sink = memorySink<NormalizedRow>()
  const progress = progressBehavior()

  const result = await pipeline()
    .from(source)
    .through(csvDecoder<CsvRow>({ header: true, browserChunkSize: 257 }))
    .through(workerTransform)
    .batch({ size: 300 })
    .to(sink)
    .run({
      runtime: "browser",
      behaviors: [progress]
    })

  const items = [...sink.getItems()].sort((left, right) => left.id - right.id)
  const metrics = result.metrics
  const hasCsvChannelMetrics = Object.keys(metrics).some((key) => key.startsWith("channel.csv-decoder#1:"))
  const snapshot = progress.getSnapshot()
  const sumAmountCents = items.reduce((sum, item) => sum + item.amountCents, 0)
  const expectedSum = expectedAmountCentsSum(rowCount)
  const first = items[0]
  const last = items.at(-1)
  const ok =
    result.ok &&
    result.errors.length === 0 &&
    items.length === rowCount &&
    first?.id === 0 &&
    first.name === "USER 0" &&
    first.amountCents === 100 &&
    last?.id === rowCount - 1 &&
    last.name === `USER ${rowCount - 1}` &&
    sumAmountCents === expectedSum &&
    snapshot.recordsHandled === rowCount * 2 &&
    snapshot.recordsWritten === rowCount &&
    !hasCsvChannelMetrics

  return {
    ok,
    rows: items.length,
    first,
    last,
    sumAmountCents,
    progress: {
      recordsRead: snapshot.recordsRead,
      recordsHandled: snapshot.recordsHandled,
      recordsWritten: snapshot.recordsWritten
    },
    hasCsvChannelMetrics,
    errors: result.errors.map((error) => `${error.code}:${error.message}`)
  }
}

function createCsv(rows: number): string {
  const lines = ["id,name,amount,note"]
  for (let index = 0; index < rows; index += 1) {
    const amount = (index + 1).toFixed(2)
    lines.push(`${index},"User ${index}",${amount},"worker path, chunk boundary ${index}"`)
  }
  return `${lines.join("\n")}\n`
}

function expectedAmountCentsSum(rows: number): number {
  return (rows * (rows + 1) * 100) / 2
}

function writeResult(result: BrowserCsvWorkerSmokeResult): void {
  if (output) {
    output.textContent = JSON.stringify(result)
  }
  ;(
    globalThis as typeof globalThis & { __IO_BROWSER_CSV_WORKER_SMOKE__?: BrowserCsvWorkerSmokeResult }
  ).__IO_BROWSER_CSV_WORKER_SMOKE__ = result
}
