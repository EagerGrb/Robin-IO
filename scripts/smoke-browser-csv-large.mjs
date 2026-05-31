import {
  findChromePath,
  getFreePort,
  runNpm,
  runPageInChrome,
  startPreview,
  waitForPreviewReady
} from "./browser-worker-harness.mjs"

const host = "127.0.0.1"
const port = process.env.BROWSER_CSV_LARGE_SMOKE_PORT
  ? Number(process.env.BROWSER_CSV_LARGE_SMOKE_PORT)
  : await getFreePort()
const rows = process.env.BROWSER_CSV_LARGE_ROWS ? Number(process.env.BROWSER_CSV_LARGE_ROWS) : 50_000
const query = new URLSearchParams({
  rows: String(rows),
  chunkSize: "4096",
  browserChunkSize: "1024",
  batchSize: "1000"
})
const url = `http://${host}:${port}/?${query}`
const workspace = "@robbin-io/example-browser-csv-smoke"

const chromePath = process.env.CHROME_PATH ?? findChromePath()
if (!chromePath) {
  throw new Error("Chrome or Edge was not found. Set CHROME_PATH to run the large browser CSV smoke test.")
}

await runNpm(["run", "build", "-w", workspace])

const preview = startPreview({ workspace, port })
try {
  await waitForPreviewReady(preview)
  const result = await runPageInChrome({
    executablePath: chromePath,
    targetUrl: url,
    resultExpression: "globalThis.__IO_BROWSER_CSV_SMOKE__",
    timeoutMs: 45_000
  })
  if (!result.ok || result.rows !== rows) {
    throw new Error(`Large browser CSV smoke test failed: ${JSON.stringify(result)}`)
  }
  console.log(
    `Large browser CSV smoke test passed: ${result.rows} rows, ${result.csvBytes} bytes, ${Math.round(
      result.durationMs
    )}ms.`
  )
} finally {
  await preview.stop()
}
