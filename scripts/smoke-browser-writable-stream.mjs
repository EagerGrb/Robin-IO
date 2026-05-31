import {
  findChromePath,
  getFreePort,
  runNpm,
  runPageInChrome,
  startPreview,
  waitForPreviewReady
} from "./browser-worker-harness.mjs"

const host = "127.0.0.1"
const port = process.env.BROWSER_WRITABLE_STREAM_SMOKE_PORT
  ? Number(process.env.BROWSER_WRITABLE_STREAM_SMOKE_PORT)
  : await getFreePort()
const url = `http://${host}:${port}/`
const workspace = "@robbin-io/example-browser-writable-stream-smoke"

const chromePath = process.env.CHROME_PATH ?? findChromePath()
if (!chromePath) {
  throw new Error("Chrome or Edge was not found. Set CHROME_PATH to run the browser WritableStream smoke test.")
}

await runNpm(["run", "build", "-w", workspace])

const preview = startPreview({ workspace, port })
try {
  await waitForPreviewReady(preview)
  const result = await runPageInChrome({
    executablePath: chromePath,
    targetUrl: url,
    resultExpression: "globalThis.__IO_BROWSER_WRITABLE_STREAM_SMOKE__"
  })
  if (!result.ok) {
    throw new Error(`Browser WritableStream smoke test failed: ${JSON.stringify(result)}`)
  }
  console.log("Browser WritableStream smoke test passed.")
} finally {
  await preview.stop()
}
