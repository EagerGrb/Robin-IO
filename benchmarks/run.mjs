import { Bench } from "tinybench"
import { Worker as NodeWorker } from "node:worker_threads"
import { pathToFileURL } from "node:url"
import { resolve } from "node:path"

const core = await importDist("core")
const sourceMemory = await importDist("source-memory")
const sinkMemory = await importDist("sink-memory")
const codecCsv = await importDist("codec-csv")
const browser = await importDist("browser")

const encoder = new TextEncoder()
const records = Array.from({ length: 10_000 }, (_, index) => ({
  id: String(index),
  name: `User ${index}`,
  score: index
}))
const largeRecords = Array.from({ length: 100_000 }, (_, index) => ({
  id: String(index),
  name: `User ${index}`,
  score: index
}))
const csvText = ["id,name,score", ...records.map((row) => `${row.id},${row.name},${row.score}`)].join("\n")
const largeCsvText = ["id,name,score", ...largeRecords.map((row) => `${row.id},${row.name},${row.score}`)].join("\n")
const transferPayloadSize = Number(process.env.BENCH_TRANSFER_PAYLOAD_SIZE ?? 256 * 1024)
const transferPayloadCount = Number(process.env.BENCH_TRANSFER_PAYLOAD_COUNT ?? 200)
const transferPayloadLabel = `${transferPayloadCount}x${formatKiB(transferPayloadSize)}KiB`
const transferWorkerUrl = new URL("./transfer-transform-worker.mjs", import.meta.url)

const bench = new Bench({
  time: Number(process.env.BENCH_TIME ?? 100),
  iterations: Number(process.env.BENCH_ITERATIONS ?? 8),
  warmupTime: Number(process.env.BENCH_WARMUP_TIME ?? 25),
  warmupIterations: Number(process.env.BENCH_WARMUP_ITERATIONS ?? 2)
})

bench.add("transform: map 10k records", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 })))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("transform benchmark pipeline failed")
  }
})

bench.add("fast-path candidate: transform map 10k records", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 })))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("transform fast-path candidate benchmark pipeline failed")
  }
})

bench.add("fast-path candidate: transform chain 10k records", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 })))
    .through(core.map((row) => ({ ...row, normalized: row.name.toLowerCase() })))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("transform chain fast-path candidate benchmark pipeline failed")
  }
})

bench.add("transform-runner: async map 10k concurrency=1", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map(asyncMapScore, { concurrency: 1 }))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("async transform concurrency=1 benchmark pipeline failed")
  }
})

bench.add("transform-runner: async map 10k concurrency=8 unordered", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map(asyncMapScore, { concurrency: 8, preserveOrder: false, maxInFlight: 8 }))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("async transform unordered benchmark pipeline failed")
  }
})

bench.add("transform-runner: async map 10k concurrency=8 ordered", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map(asyncMapScore, { concurrency: 8, preserveOrder: true, maxInFlight: 8 }))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("async transform ordered benchmark pipeline failed")
  }
})

bench.add("channel path: sync map 10k concurrency=8", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 }), { concurrency: 8, maxInFlight: 8 }))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("sync transform channel-path benchmark pipeline failed")
  }
})

bench.add("observer: map 10k records full events", async () => {
  const sink = sinkMemory.memorySink()
  let eventCount = 0
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 })))
    .batch({ size: 1000 })
    .to(sink)
    .run({
      runtime: "node",
      onEvent() {
        eventCount += 1
      }
    })

  if (!result.ok || sink.getItems().length !== records.length || eventCount === 0) {
    throw new Error("observer benchmark pipeline failed")
  }
})

bench.add("observer-cost: map 10k full events type counts", async () => {
  const sink = sinkMemory.memorySink()
  const eventTypes = new Map()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 })))
    .batch({ size: 1000 })
    .to(sink)
    .run({
      runtime: "node",
      onEvent(event) {
        eventTypes.set(event.type, (eventTypes.get(event.type) ?? 0) + 1)
      }
    })

  if (
    !result.ok ||
    sink.getItems().length !== records.length ||
    (eventTypes.get("stage.item.end") ?? 0) < records.length
  ) {
    throw new Error("observer type-count benchmark pipeline failed")
  }
})

bench.add("observer-cost: map 10k full events async noop", async () => {
  const sink = sinkMemory.memorySink()
  let eventCount = 0
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 })))
    .batch({ size: 1000 })
    .to(sink)
    .run({
      runtime: "node",
      async onEvent() {
        eventCount += 1
      }
    })

  if (!result.ok || sink.getItems().length !== records.length || eventCount === 0) {
    throw new Error("observer async-noop benchmark pipeline failed")
  }
})

bench.add("channel path: sync map 10k concurrency=8 full events", async () => {
  const sink = sinkMemory.memorySink()
  let channelEvents = 0
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 }), { concurrency: 8, maxInFlight: 8 }))
    .batch({ size: 1000 })
    .to(sink)
    .run({
      runtime: "node",
      onEvent(event) {
        if (event.type.startsWith("channel.")) {
          channelEvents += 1
        }
      }
    })

  if (!result.ok || sink.getItems().length !== records.length || channelEvents === 0) {
    throw new Error("observed sync transform channel-path benchmark pipeline failed")
  }
})

bench.add("behavior: progress map 10k records", async () => {
  const sink = sinkMemory.memorySink()
  const progress = core.progressBehavior()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .through(core.map((row) => ({ ...row, score: row.score + 1 })))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node", behaviors: [progress] })

  if (
    !result.ok ||
    sink.getItems().length !== records.length ||
    progress.getSnapshot().recordsRead !== records.length
  ) {
    throw new Error("progress behavior benchmark pipeline failed")
  }
})

bench.add("csv: decode 10k rows", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource([encoder.encode(csvText)]))
    .through(codecCsv.csvDecoder({ header: true }))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("csv benchmark pipeline failed")
  }
})

bench.add("codec candidate: csv decode 100k rows", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource([encoder.encode(largeCsvText)]))
    .through(codecCsv.csvDecoder({ header: true }))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== largeRecords.length) {
    throw new Error("large csv benchmark pipeline failed")
  }
})

bench.add("batch: write 10k records", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== records.length) {
    throw new Error("batch benchmark pipeline failed")
  }
})

bench.add("pipeline: map+batch+memory 100k records", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(largeRecords))
    .through(core.map((row) => ({ ...row, score: row.score + 1 })))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== largeRecords.length) {
    throw new Error("large memory pipeline benchmark failed")
  }
})

bench.add("fast-path candidate: batch/sink 100k records", async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(largeRecords))
    .batch({ size: 1000 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || sink.getItems().length !== largeRecords.length) {
    throw new Error("batch/sink fast-path candidate benchmark pipeline failed")
  }
})

bench.add("pipeline: slow sink 10k records 1ms/batch", async () => {
  let written = 0
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(records))
    .batch({ size: 1000 })
    .to({
      kind: "sink",
      name: "slow-sink",
      async write(batch) {
        await delay(1)
        written += batch.items.length
      }
    })
    .run({ runtime: "node" })

  if (!result.ok || written !== records.length) {
    throw new Error("slow sink benchmark pipeline failed")
  }
})

bench.add(`browser worker: ArrayBuffer clone ${transferPayloadLabel}`, async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(() => createTransferPayloads()))
    .through(
      browser.webWorkerTransform(() => createBenchmarkWorker(), {
        name: "worker-clone",
        concurrency: 8,
        maxPending: 16
      })
    )
    .batch({ size: 100 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || totalNumbers(sink.getItems()) !== transferPayloadSize * transferPayloadCount) {
    throw new Error("worker ArrayBuffer clone benchmark pipeline failed")
  }
})

bench.add(`browser worker: ArrayBuffer transfer ${transferPayloadLabel}`, async () => {
  const sink = sinkMemory.memorySink()
  const result = await core
    .pipeline()
    .from(sourceMemory.memorySource(() => createTransferPayloads()))
    .through(
      browser.webWorkerTransform(() => createBenchmarkWorker(), {
        name: "worker-transfer",
        concurrency: 8,
        maxPending: 16,
        transfer(input) {
          return [input]
        }
      })
    )
    .batch({ size: 100 })
    .to(sink)
    .run({ runtime: "node" })

  if (!result.ok || totalNumbers(sink.getItems()) !== transferPayloadSize * transferPayloadCount) {
    throw new Error("worker ArrayBuffer transfer benchmark pipeline failed")
  }
})

await bench.run()

console.table(
  bench.tasks.map((task) => ({
    name: task.name,
    "ops/sec": Math.round(task.result?.hz ?? 0),
    "mean ms": Number((task.result?.mean ?? 0).toFixed(3)),
    samples: task.result?.samples.length ?? 0
  }))
)

async function importDist(name) {
  return import(pathToFileURL(resolve("packages", name, "dist", "index.js")).href)
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function asyncMapScore(row) {
  return { ...row, score: row.score + 1 }
}

function createTransferPayloads() {
  return Array.from({ length: transferPayloadCount }, () => new ArrayBuffer(transferPayloadSize))
}

function createBenchmarkWorker() {
  const worker = new NodeWorker(transferWorkerUrl, { type: "module" })
  return {
    addEventListener(type, listener) {
      if (type === "message") {
        worker.on("message", (data) => listener({ data }))
        return
      }
      if (type === "error") {
        worker.on("error", listener)
      }
    },
    postMessage(message, transfer) {
      worker.postMessage(message, transfer)
    },
    terminate() {
      void worker.terminate()
    }
  }
}

function totalNumbers(values) {
  return values.reduce((sum, value) => sum + value, 0)
}

function formatKiB(bytes) {
  return Number((bytes / 1024).toFixed(2))
}
