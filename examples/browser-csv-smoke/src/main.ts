import { blobSource } from "@robbin-io/browser"
import { csvDecoder } from "@robbin-io/codec-csv"
import { pipeline, progressBehavior } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"

interface BrowserCsvSmokeResult {
  readonly ok: boolean
  readonly rows: number
  readonly expectedRows: number
  readonly csvBytes: number
  readonly durationMs: number
  readonly heapUsedBytes?: number
  readonly firstName?: string
  readonly lastName?: string
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
      expectedRows: 0,
      csvBytes: 0,
      durationMs: 0,
      progress: {
        recordsRead: 0,
        recordsHandled: 0,
        recordsWritten: 0
      },
      hasCsvChannelMetrics: false,
      errors: [error instanceof Error ? error.message : String(error)]
    })
  )

async function runSmoke(): Promise<BrowserCsvSmokeResult> {
  const options = readSmokeOptions()
  const rowCount = options.rows
  const csv = createCsv(rowCount)
  const csvBytes = new TextEncoder().encode(csv).byteLength
  const source = blobSource(new Blob([csv], { type: "text/csv" }), {
    name: "browser-csv-blob",
    chunkSize: options.chunkSize
  })
  const sink = memorySink<Record<string, string>>()
  const progress = progressBehavior()
  const startedAt = performance.now()

  const result = await pipeline()
    .from(source)
    .through(csvDecoder({ header: true, browserChunkSize: options.browserChunkSize }))
    .batch({ size: options.batchSize })
    .to(sink)
    .run({
      runtime: "browser",
      behaviors: [progress]
    })
  const durationMs = performance.now() - startedAt

  const items = sink.getItems()
  const metrics = result.metrics
  const hasCsvChannelMetrics = Object.keys(metrics).some((key) => key.startsWith("channel.csv-decoder#1:"))
  const snapshot = progress.getSnapshot()
  const ok =
    result.ok &&
    items.length === rowCount &&
    items[0]?.name === "User 0" &&
    items.at(-1)?.name === `User ${rowCount - 1}` &&
    snapshot.recordsHandled === rowCount &&
    snapshot.recordsWritten === rowCount &&
    !hasCsvChannelMetrics

  return {
    ok,
    rows: items.length,
    expectedRows: rowCount,
    csvBytes,
    durationMs,
    heapUsedBytes: readHeapUsedBytes(),
    firstName: items[0]?.name,
    lastName: items.at(-1)?.name,
    progress: {
      recordsRead: snapshot.recordsRead,
      recordsHandled: snapshot.recordsHandled,
      recordsWritten: snapshot.recordsWritten
    },
    hasCsvChannelMetrics,
    errors: result.errors.map((error) => `${error.code}:${error.message}`)
  }
}

function readSmokeOptions(): { rows: number; chunkSize: number; browserChunkSize: number; batchSize: number } {
  const params = new URLSearchParams(globalThis.location.search)
  return {
    rows: readPositiveInteger(params, "rows", 5000),
    chunkSize: readPositiveInteger(params, "chunkSize", 1024),
    browserChunkSize: readPositiveInteger(params, "browserChunkSize", 512),
    batchSize: readPositiveInteger(params, "batchSize", 250)
  }
}

function readPositiveInteger(params: URLSearchParams, name: string, fallback: number): number {
  const value = Number(params.get(name))
  return Number.isInteger(value) && value > 0 ? value : fallback
}

function readHeapUsedBytes(): number | undefined {
  const memory = (performance as Performance & { memory?: { usedJSHeapSize?: number } }).memory
  return typeof memory?.usedJSHeapSize === "number" ? memory.usedJSHeapSize : undefined
}

function createCsv(rows: number): string {
  const lines = ["id,name,note"]
  for (let index = 0; index < rows; index += 1) {
    lines.push(`${index},"User ${index}","line ${index}, quoted"`)
  }
  return `${lines.join("\n")}\n`
}

function writeResult(result: BrowserCsvSmokeResult): void {
  if (output) {
    output.textContent = JSON.stringify(result)
  }
  ;(globalThis as typeof globalThis & { __IO_BROWSER_CSV_SMOKE__?: BrowserCsvSmokeResult }).__IO_BROWSER_CSV_SMOKE__ =
    result
}
