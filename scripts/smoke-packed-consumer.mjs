import { spawn } from "node:child_process"
import { existsSync } from "node:fs"
import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

const npmCommand = resolveNpmCommand()
const rootDir = resolve(".")
const packagesRoot = resolve("packages")
const packDir = await mkdtemp(join(tmpdir(), "io-packed-tarballs-"))
const consumerDir = await mkdtemp(join(tmpdir(), "io-packed-consumer-"))
const keepTemp = process.env.KEEP_PACKED_CONSUMER_SMOKE === "1"

try {
  const packages = await readWorkspacePackages()
  const tarballs = []
  for (const packageJson of packages) {
    const packed = await npmPack(packageJson.name)
    tarballs.push(join(packDir, packed.filename))
  }

  await writeFile(
    join(consumerDir, "package.json"),
    JSON.stringify(
      {
        name: "io-packed-consumer-smoke",
        private: true,
        type: "module",
        scripts: {
          smoke: "node smoke.mjs"
        }
      },
      null,
      2
    )
  )
  await writeFile(join(consumerDir, "smoke.mjs"), consumerSmokeSource())

  await runNpm(["install", "--ignore-scripts", "--no-audit", "--no-fund", ...tarballs], consumerDir)
  await runNpm(["run", "smoke"], consumerDir)

  console.log(`Packed consumer smoke passed for ${packages.length} packages.`)
} catch (error) {
  console.error(`Packed consumer smoke failed. Temp directories kept for inspection:`)
  console.error(`- tarballs: ${packDir}`)
  console.error(`- consumer: ${consumerDir}`)
  throw error
} finally {
  if (!keepTemp) {
    await rm(packDir, { recursive: true, force: true }).catch(() => undefined)
    await rm(consumerDir, { recursive: true, force: true }).catch(() => undefined)
  }
}

async function readWorkspacePackages() {
  const entries = await readdir(packagesRoot, { withFileTypes: true })
  const packages = []
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue
    }
    const packageJsonPath = join(packagesRoot, entry.name, "package.json")
    if (!existsSync(packageJsonPath)) {
      continue
    }
    packages.push(JSON.parse(await readFile(packageJsonPath, "utf8")))
  }
  return packages.sort((left, right) => left.name.localeCompare(right.name))
}

async function npmPack(workspace) {
  const output = await runNpm(["pack", "--json", "--workspace", workspace, "--pack-destination", packDir], rootDir)
  const parsed = JSON.parse(output)
  const packed = parsed[0]
  if (!packed?.filename) {
    throw new Error(`npm pack returned no filename for ${workspace}: ${output}`)
  }
  return packed
}

async function runNpm(args, cwd) {
  return await new Promise((resolve, reject) => {
    let stdout = ""
    let stderr = ""
    const child = spawn(npmCommand.command, [...npmCommand.argsPrefix, ...args], {
      cwd,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"]
    })
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk)
    })
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk)
    })
    child.on("error", reject)
    child.on("exit", (code) => {
      if (code === 0) {
        resolve(stdout)
        return
      }
      reject(new Error(`npm ${args.join(" ")} failed with exit code ${code}: ${stderr}${stdout}`))
    })
  })
}

function consumerSmokeSource() {
  return String.raw`
import { readFile, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { blobSource, writableStreamSink } from "@robbin-io/browser"
import { csvDecoder, csvEncoder } from "@robbin-io/codec-csv"
import { jsonlDecoder, jsonlEncoder } from "@robbin-io/codec-jsonl"
import { map, pipeline, progressBehavior } from "@robbin-io/core"
import { fsFileSink, fsFileSource } from "@robbin-io/node"
import { memorySink } from "@robbin-io/sink-memory"
import { memorySource } from "@robbin-io/source-memory"
import { mapFields } from "@robbin-io/transform-fields"
import { validateWithZod } from "@robbin-io/validation-zod"
import { z } from "zod"

await smokeMemoryAndCodecs()
await smokeNodeFiles()
await smokeBrowserAdapters()

async function smokeMemoryAndCodecs() {
  const sink = memorySink()
  const progress = progressBehavior()
  const result = await pipeline()
    .from(memorySource([new TextEncoder().encode("id,name\n1,Ada\n2,Grace\n")]))
    .through(csvDecoder({ header: true }))
    .through(
      mapFields({
        id: { from: "id", required: true },
        name: { from: "name", required: true, transform: (value) => String(value).trim() }
      })
    )
    .through(validateWithZod(z.object({ id: z.string(), name: z.string() })))
    .through(map((row) => ({ ...row, name: row.name.toUpperCase() })))
    .through(jsonlEncoder())
    .to(sink)
    .run({ runtime: "node", behaviors: [progress] })

  assert(result.ok, "CSV -> mapFields -> zod -> JSONL pipeline failed")
  assert(progress.getSnapshot().recordsWritten > 0, "progress behavior did not observe writes")

  const decoded = memorySink()
  const decodeResult = await pipeline()
    .from(memorySource(sink.getItems()))
    .through(jsonlDecoder())
    .to(decoded)
    .run({ runtime: "node" })
  assert(decodeResult.ok, "JSONL decode failed")
  assert(decoded.getItems().length === 2, "JSONL row count mismatch")
  assert(decoded.getItems()[0].name === "ADA", "transform output mismatch")

  const csvBytes = memorySink()
  const encodeResult = await pipeline()
    .from(memorySource(decoded.getItems()))
    .through(csvEncoder({ header: ["id", "name"], escapeFormula: true }))
    .to(csvBytes)
    .run({ runtime: "node" })
  assert(encodeResult.ok, "CSV encode failed")
  assert(new TextDecoder().decode(joinBytes(csvBytes.getItems())).includes("ADA"), "CSV encode output mismatch")
}

async function smokeNodeFiles() {
  const inputPath = join(process.cwd(), "input.txt")
  const outputPath = join(process.cwd(), "out", "output.txt")
  await writeFile(inputPath, "packed consumer")
  const result = await pipeline()
    .from(fsFileSource(inputPath))
    .to(fsFileSink(outputPath, { atomic: true, createParentDirectories: true }))
    .run({ runtime: "node" })
  assert(result.ok, "Node file copy pipeline failed")
  assert((await readFile(outputPath, "utf8")) === "packed consumer", "Node file output mismatch")
}

async function smokeBrowserAdapters() {
  const blobChunks = memorySink()
  const blobResult = await pipeline()
    .from(blobSource(new Blob(["abc"]), { chunkSize: 1 }))
    .to(blobChunks)
    .run({ runtime: "browser" })
  assert(blobResult.ok, "blobSource pipeline failed")
  assert(new TextDecoder().decode(joinBytes(blobChunks.getItems())) === "abc", "blobSource output mismatch")

  let text = ""
  let closed = false
  const stream = new WritableStream({
    write(chunk) {
      text += typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk)
    },
    close() {
      closed = true
    }
  })
  const writable = writableStreamSink(stream)
  const streamResult = await pipeline()
    .from(memorySource(["hello ", new TextEncoder().encode("stream")]))
    .to(writable)
    .run({ runtime: "browser" })
  assert(streamResult.ok, "writableStreamSink pipeline failed")
  assert(text === "hello stream", "WritableStream output mismatch")
  assert(closed, "WritableStream was not closed")
  assert(writable.getBytesWritten() === 12, "WritableStream byte count mismatch")
}

function joinBytes(chunks) {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
  const output = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    output.set(chunk, offset)
    offset += chunk.byteLength
  }
  return output
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message)
  }
}
`
}

function resolveNpmCommand() {
  if (process.env.npm_execpath) {
    return { command: process.execPath, argsPrefix: [process.env.npm_execpath] }
  }
  if (process.platform === "win32") {
    return { command: process.env.ComSpec ?? "cmd.exe", argsPrefix: ["/d", "/s", "/c", "npm"] }
  }
  return { command: "npm", argsPrefix: [] }
}
