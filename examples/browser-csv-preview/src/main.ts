import { pipeline, progressBehavior, type ProgressBehavior, type RuntimeBehavior } from "@robbin-io/core"
import { fileSource, webWorkerTransform } from "@robbin-io/browser"
import { csvDecoder } from "@robbin-io/codec-csv"
import { memorySink } from "@robbin-io/sink-memory"

const input = document.querySelector<HTMLInputElement>("#file")!
const state = document.querySelector<HTMLElement>("#state")!
const recordsRead = document.querySelector<HTMLElement>("#records-read")!
const rows = document.querySelector<HTMLElement>("#rows")!
const batches = document.querySelector<HTMLElement>("#batches")!
const errors = document.querySelector<HTMLElement>("#errors")!
const status = document.querySelector<HTMLPreElement>("#status")!
const preview = document.querySelector<HTMLTableElement>("#preview")!

input.addEventListener("change", async () => {
  const file = input.files?.[0]
  if (!file) return

  state.textContent = "Running"
  renderPreview([])
  renderSummary({ read: 0, rows: 0, batches: 0, errors: 0 })
  status.textContent = JSON.stringify({ file: file.name, size: file.size }, null, 2)

  const sink = memorySink<Record<string, string>>()
  const progress = progressBehavior()
  const progressUi = progressUiBehavior(progress, sink)
  const normalizeRows = webWorkerTransform<Record<string, string>, Record<string, string>>(
    () => new Worker(new URL("./row-normalize.worker.ts", import.meta.url), { type: "module" }),
    { name: "normalize-rows", concurrency: 8 }
  )
  const result = await pipeline()
    .from(fileSource(file))
    .through(csvDecoder({ header: true }))
    .through(normalizeRows)
    .batch({ size: 500 })
    .to(sink)
    .run({
      runtime: "browser",
      behaviors: [progress, progressUi],
      errorMode: "skip-and-collect"
    })

  const items = sink.getItems()
  state.textContent = result.ok ? "Done" : "Done"
  renderSummary({
    read: progress.getSnapshot().recordsRead,
    rows: items.length,
    batches: progress.getSnapshot().batchesWritten,
    errors: result.errors.length
  })
  status.textContent = JSON.stringify(
    {
      ok: result.ok,
      file: file.name,
      progress: progress.getSnapshot(),
      errors: result.errors.map((error) => ({
        code: error.code,
        message: error.message,
        stage: error.stage,
        metadata: error.metadata
      }))
    },
    null,
    2
  )
  renderPreview(sink.getItems().slice(0, 20))
})

function progressUiBehavior(
  progress: ProgressBehavior,
  sink: ReturnType<typeof memorySink<Record<string, string>>>
): RuntimeBehavior {
  let pending = false
  const schedule = (): void => {
    if (pending) {
      return
    }
    pending = true
    requestAnimationFrame(() => {
      pending = false
      const snapshot = progress.getSnapshot()
      renderSummary({
        read: snapshot.recordsRead,
        rows: sink.getItems().length,
        batches: snapshot.batchesWritten,
        errors: snapshot.errors
      })
    })
  }

  return {
    name: "progress-ui",
    onRecord: schedule,
    onBatch: schedule,
    onError: schedule,
    onFinish: schedule
  }
}

function renderSummary(summary: { read: number; rows: number; batches: number; errors: number }): void {
  recordsRead.textContent = String(summary.read)
  rows.textContent = String(summary.rows)
  batches.textContent = String(summary.batches)
  errors.textContent = String(summary.errors)
}

function renderPreview(rows: readonly Record<string, string>[]): void {
  preview.replaceChildren()
  const columns = Object.keys(rows[0] ?? {})
  if (columns.length === 0) {
    return
  }
  const header = preview.createTHead().insertRow()
  for (const column of columns) {
    const cell = document.createElement("th")
    cell.textContent = column
    header.append(cell)
  }

  const body = preview.createTBody()
  for (const row of rows) {
    const tr = body.insertRow()
    for (const column of columns) {
      tr.insertCell().textContent = row[column] ?? ""
    }
  }
}
