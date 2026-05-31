import { chromium, firefox, webkit } from "@playwright/test"
import { getFreePort, runNpm, startPreview, waitForPreviewReady } from "./browser-worker-harness.mjs"

const browserTypes = {
  chromium,
  firefox,
  webkit
}

const cases = [
  {
    id: "csv",
    workspace: "@robbin-io/example-browser-csv-smoke",
    globalName: "__IO_BROWSER_CSV_SMOKE__"
  },
  {
    id: "csv-worker",
    workspace: "@robbin-io/example-browser-csv-worker-smoke",
    globalName: "__IO_BROWSER_CSV_WORKER_SMOKE__"
  },
  {
    id: "writable-stream",
    workspace: "@robbin-io/example-browser-writable-stream-smoke",
    globalName: "__IO_BROWSER_WRITABLE_STREAM_SMOKE__"
  },
  {
    id: "worker",
    workspace: "@robbin-io/example-browser-worker-smoke",
    globalName: "__IO_BROWSER_WORKER_SMOKE__"
  }
]

const selectedBrowsers = readList("BROWSER_MATRIX_BROWSERS", "chromium,firefox,webkit")
const selectedCases = readList("BROWSER_MATRIX_CASES", cases.map((testCase) => testCase.id).join(","))
const timeoutMs = readPositiveInteger("BROWSER_MATRIX_TIMEOUT_MS", 30_000)
const host = "127.0.0.1"

for (const browserName of selectedBrowsers) {
  if (!(browserName in browserTypes)) {
    throw new Error(`Unknown browser "${browserName}". Expected one of: ${Object.keys(browserTypes).join(", ")}`)
  }
}

const activeCases = cases.filter((testCase) => selectedCases.includes(testCase.id))
if (activeCases.length === 0) {
  throw new Error(
    `No browser smoke cases selected. Available cases: ${cases.map((testCase) => testCase.id).join(", ")}`
  )
}

for (const testCase of activeCases) {
  await runNpm(["run", "build", "-w", testCase.workspace])
  const port = await getFreePort()
  const preview = startPreview({ workspace: testCase.workspace, port })
  try {
    await waitForPreviewReady(preview)
    const targetUrl = `http://${host}:${port}/`
    for (const browserName of selectedBrowsers) {
      const result = await runCaseInBrowser({ browserName, targetUrl, globalName: testCase.globalName, timeoutMs })
      if (!result.ok) {
        throw new Error(`Browser matrix smoke failed for ${testCase.id} on ${browserName}: ${JSON.stringify(result)}`)
      }
      console.log(`Browser matrix smoke passed for ${testCase.id} on ${browserName}.`)
    }
  } finally {
    await preview.stop()
  }
}

console.log(`Browser matrix smoke passed for ${activeCases.length} case(s) on ${selectedBrowsers.length} browser(s).`)

async function runCaseInBrowser({ browserName, targetUrl, globalName, timeoutMs }) {
  const launchOptions = launchOptionsFor(browserName)
  const browser = await browserTypes[browserName].launch(launchOptions)
  try {
    const page = await browser.newPage()
    await page.goto(targetUrl, { waitUntil: "domcontentloaded" })
    await page.waitForFunction((name) => Boolean(globalThis[name]), globalName, { timeout: timeoutMs })
    return await page.evaluate((name) => globalThis[name], globalName)
  } finally {
    await browser.close()
  }
}

function launchOptionsFor(browserName) {
  if (browserName === "chromium" && process.env.PLAYWRIGHT_CHROMIUM_CHANNEL) {
    return { channel: process.env.PLAYWRIGHT_CHROMIUM_CHANNEL }
  }
  return {}
}

function readList(name, fallback) {
  const value = process.env[name] ?? fallback
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
}

function readPositiveInteger(name, fallback) {
  const value = Number(process.env[name])
  return Number.isInteger(value) && value > 0 ? value : fallback
}
