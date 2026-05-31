import { createReadStream, createWriteStream } from "node:fs"
import { mkdir, rm, stat } from "node:fs/promises"
import { once } from "node:events"
import { createInterface } from "node:readline/promises"
import { performance } from "node:perf_hooks"
import { resolve } from "node:path"
import { pathToFileURL } from "node:url"

const core = await importDist("core")
const codecCsv = await importDist("codec-csv")
const codecJsonl = await importDist("codec-jsonl")
const node = await importDist("node")

const rows = Number(process.env.STRESS_ROWS ?? 1_000_000)
const batchSize = Number(process.env.STRESS_BATCH_SIZE ?? 5000)
const chunkSize = Number(process.env.STRESS_CHUNK_SIZE ?? 1024 * 1024)
const encoderChunkBytes = Number(process.env.STRESS_ENCODER_CHUNK_BYTES ?? 64 * 1024)
const outputBatchSize = Number(process.env.STRESS_OUTPUT_BATCH_SIZE ?? (encoderChunkBytes > 0 ? 64 : batchSize))
const outputDir = resolve(process.env.STRESS_OUTPUT_DIR ?? "benchmarks/.stress-output")
const inputCsv = resolve(outputDir, `input-${rows}.csv`)
const outputJsonl = resolve(outputDir, `output-${rows}.jsonl`)
const roundtripCsv = resolve(outputDir, `roundtrip-${rows}.csv`)

await mkdir(outputDir, { recursive: true })
await rm(inputCsv, { force: true })
await rm(outputJsonl, { force: true })
await rm(roundtripCsv, { force: true })

console.log(`Generating ${rows.toLocaleString("en-US")} CSV rows...`)
const generated = await measure(async () => {
  await generateCsv(inputCsv, rows)
})

const inputSize = await fileSize(inputCsv)
console.log(`Input CSV: ${formatBytes(inputSize)} at ${inputCsv}`)

console.log("Running CSV -> transform -> JSONL import/export pipeline...")
const csvToJsonl = await measurePipeline(async () => {
  const progress = core.progressBehavior()
  const result = await core
    .pipeline()
    .from(node.fsFileSource(inputCsv, { chunkSize }))
    .through(codecCsv.csvDecoder({ header: true }))
    .through(
      core.map((row) => ({
        id: Number(row.id),
        name: String(row.name ?? "").trim(),
        email: String(row.email ?? "").toLowerCase(),
        amount: Number(row.amount),
        active: row.active === "true",
        region: row.region,
        bucket: Number(row.id) % 128
      }))
    )
    .through(codecJsonl.jsonlEncoder({ chunkBytes: encoderChunkBytes }))
    .batch({ size: outputBatchSize })
    .to(node.fsFileSink(outputJsonl, { createParentDirectories: true, atomic: true }))
    .run({ runtime: "node", behaviors: [progress], errorMode: "skip-and-collect" })

  return { result, progress: progress.getSnapshot() }
})

const outputJsonlSize = await fileSize(outputJsonl)
const outputJsonlLines = await countLines(outputJsonl)

console.log("Running JSONL -> transform -> CSV export pipeline...")
const jsonlToCsv = await measurePipeline(async () => {
  const progress = core.progressBehavior()
  const result = await core
    .pipeline()
    .from(node.fsFileSource(outputJsonl, { chunkSize }))
    .through(codecJsonl.jsonlDecoder())
    .through(
      core.map((row) => ({
        id: row.id,
        name: row.name,
        email: row.email,
        amount: row.amount,
        active: row.active,
        region: row.region,
        bucket: row.bucket
      }))
    )
    .through(
      codecCsv.csvEncoder({
        header: ["id", "name", "email", "amount", "active", "region", "bucket"],
        chunkBytes: encoderChunkBytes
      })
    )
    .batch({ size: outputBatchSize })
    .to(node.fsFileSink(roundtripCsv, { createParentDirectories: true, atomic: true }))
    .run({ runtime: "node", behaviors: [progress], errorMode: "skip-and-collect" })

  return { result, progress: progress.getSnapshot() }
})

const roundtripCsvSize = await fileSize(roundtripCsv)
const roundtripCsvLines = await countLines(roundtripCsv)

const summary = {
  timestamp: new Date().toISOString(),
  node: process.version,
  platform: `${process.platform} ${process.arch}`,
  rows,
  batchSize,
  outputBatchSize,
  chunkSize,
  encoderChunkBytes,
  files: {
    inputCsv,
    outputJsonl,
    roundtripCsv
  },
  generateCsv: {
    durationMs: round(generated.durationMs),
    inputBytes: inputSize,
    inputMiB: round(toMiB(inputSize))
  },
  csvToJsonl: summarizeRun(csvToJsonl, rows, inputSize, outputJsonlSize, outputJsonlLines),
  jsonlToCsv: summarizeRun(jsonlToCsv, rows, outputJsonlSize, roundtripCsvSize, roundtripCsvLines - 1)
}

console.log(JSON.stringify(summary, null, 2))

function summarizeRun(run, expectedRows, inputBytes, outputBytes, outputRows) {
  const { result, progress } = run.value
  return {
    ok: result.ok,
    errors: result.errors.length,
    durationMs: round(run.durationMs),
    throughputRowsPerSec: round(expectedRows / (run.durationMs / 1000)),
    inputMiBPerSec: round(toMiB(inputBytes) / (run.durationMs / 1000)),
    outputMiBPerSec: round(toMiB(outputBytes) / (run.durationMs / 1000)),
    inputBytes,
    outputBytes,
    outputRows,
    progress,
    memory: {
      peakRssMiB: round(toMiB(run.peak.rss)),
      peakHeapUsedMiB: round(toMiB(run.peak.heapUsed)),
      finalRssMiB: round(toMiB(run.final.rss)),
      finalHeapUsedMiB: round(toMiB(run.final.heapUsed))
    },
    selectedMetrics: pickMetrics(result.metrics)
  }
}

async function measurePipeline(fn) {
  const peak = process.memoryUsage()
  const sampler = setInterval(() => {
    const current = process.memoryUsage()
    peak.rss = Math.max(peak.rss, current.rss)
    peak.heapTotal = Math.max(peak.heapTotal, current.heapTotal)
    peak.heapUsed = Math.max(peak.heapUsed, current.heapUsed)
    peak.external = Math.max(peak.external, current.external)
    peak.arrayBuffers = Math.max(peak.arrayBuffers, current.arrayBuffers)
  }, 25)
  sampler.unref()

  try {
    const measured = await measure(fn)
    return {
      ...measured,
      peak,
      final: process.memoryUsage()
    }
  } finally {
    clearInterval(sampler)
  }
}

async function measure(fn) {
  const started = performance.now()
  const value = await fn()
  return {
    durationMs: performance.now() - started,
    value
  }
}

async function generateCsv(path, totalRows) {
  const stream = createWriteStream(path)
  stream.write("id,name,email,amount,active,region\n")

  for (let index = 0; index < totalRows; index += 1) {
    const line = [
      index,
      csvValue(`User ${index}`),
      `user${index}@example.com`,
      (index * 17.13).toFixed(2),
      index % 3 !== 0 ? "true" : "false",
      `region-${index % 12}`
    ].join(",")

    if (!stream.write(`${line}\n`)) {
      await once(stream, "drain")
    }
  }

  stream.end()
  await once(stream, "finish")
}

async function countLines(path) {
  let lines = 0
  const rl = createInterface({
    input: createReadStream(path),
    crlfDelay: Infinity
  })
  for await (const _line of rl) {
    lines += 1
  }
  return lines
}

async function fileSize(path) {
  return (await stat(path)).size
}

async function importDist(name) {
  return import(pathToFileURL(resolve("packages", name, "dist", "index.js")).href)
}

function csvValue(value) {
  return /[",\r\n]/.test(value) ? `"${value.replaceAll('"', '""')}"` : value
}

function pickMetrics(metrics) {
  return Object.fromEntries(
    ["records.read", "records.handled", "records.written", "batches.written", "bytes.written", "errors"].flatMap(
      (key) => (metrics[key] === undefined ? [] : [[key, metrics[key]]])
    )
  )
}

function formatBytes(bytes) {
  return `${round(toMiB(bytes))} MiB`
}

function toMiB(bytes) {
  return bytes / 1024 / 1024
}

function round(value) {
  return Math.round(value * 100) / 100
}
