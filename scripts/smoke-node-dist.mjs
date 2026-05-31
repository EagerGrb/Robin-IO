import { existsSync } from "node:fs"
import { pathToFileURL } from "node:url"
import { resolve } from "node:path"

const packageNames = [
  "core",
  "codec-jsonl",
  "source-memory",
  "sink-memory",
  "node",
  "browser",
  "source-file",
  "sink-file",
  "codec-csv",
  "plugin-testing",
  "transform-fields",
  "validation-zod"
]

for (const name of packageNames) {
  const entry = resolve("packages", name, "dist", "index.js")
  if (!existsSync(entry)) {
    throw new Error(`Missing dist entry for ${name}: ${entry}`)
  }
  await import(pathToFileURL(entry).href)
}

const core = await import(pathToFileURL(resolve("packages/core/dist/index.js")).href)
const sourceMemory = await import(pathToFileURL(resolve("packages/source-memory/dist/index.js")).href)
const sinkMemory = await import(pathToFileURL(resolve("packages/sink-memory/dist/index.js")).href)

const sink = sinkMemory.memorySink()
const result = await core
  .pipeline()
  .from(sourceMemory.memorySource([1, 2, 3]))
  .through(core.map((value) => value * 2))
  .batch({ size: 2 })
  .to(sink)
  .run({ runtime: "node" })

if (!result.ok) {
  throw new Error(`Dist smoke pipeline failed: ${result.errors.map((error) => error.message).join("; ")}`)
}

const items = sink.getItems()
if (JSON.stringify(items) !== JSON.stringify([2, 4, 6])) {
  throw new Error(`Unexpected dist smoke output: ${JSON.stringify(items)}`)
}

console.log("Node ESM dist smoke test passed.")
