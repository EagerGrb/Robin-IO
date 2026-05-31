import {
  findChromePath,
  getFreePort,
  runNpm,
  runPageInChrome,
  startPreview,
  waitForPreviewReady
} from "./browser-worker-harness.mjs"

const host = "127.0.0.1"
const port = process.env.BROWSER_WORKER_BENCH_PORT ? Number(process.env.BROWSER_WORKER_BENCH_PORT) : await getFreePort()
const payloadCount = Number(process.env.BROWSER_WORKER_BENCH_COUNT ?? 10)
const payloadSize = Number(process.env.BROWSER_WORKER_BENCH_SIZE ?? 64 * 1024)
const iterations = Number(process.env.BROWSER_WORKER_BENCH_ITERATIONS ?? 2)
const url = `http://${host}:${port}/?count=${payloadCount}&size=${payloadSize}&iterations=${iterations}`
const workspace = "@robbin-io/example-browser-worker-bench"

const chromePath = process.env.CHROME_PATH ?? findChromePath()
if (!chromePath) {
  throw new Error("Chrome or Edge was not found. Set CHROME_PATH to run the browser worker benchmark.")
}

console.log("Building browser worker benchmark fixture...")
await runNpm(["run", "build", "-w", workspace])

console.log(`Starting browser worker benchmark preview at ${url}...`)
const preview = startPreview({ workspace, port })
try {
  await waitForPreviewReady(preview)
  console.log("Running browser worker benchmark in headless Chrome...")
  const result = await runPageInChrome({
    executablePath: chromePath,
    targetUrl: url,
    resultExpression: "globalThis.__IO_BROWSER_WORKER_BENCH__",
    timeoutMs: 60_000
  })
  if (!result.ok) {
    throw new Error(`Browser worker benchmark failed: ${JSON.stringify(result)}`)
  }

  console.table(
    result.results.map((entry) => ({
      name: `browser worker: ArrayBuffer ${entry.name} ${result.payloadCount}x${formatKiB(result.payloadSize)}KiB`,
      "mean ms": Number(entry.meanMs.toFixed(3)),
      samples: entry.samples.length
    }))
  )
} finally {
  await preview.stop()
}

function formatKiB(bytes) {
  return Number((bytes / 1024).toFixed(2))
}
