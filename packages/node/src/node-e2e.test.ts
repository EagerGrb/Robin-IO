import { mkdir, readFile, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { PassThrough, Writable } from "node:stream"
import { afterEach, describe, expect, it } from "vitest"
import { map, pipeline, progressBehavior } from "@robbin-io/core"
import { csvDecoder } from "@robbin-io/codec-csv"
import { jsonlEncoder } from "@robbin-io/codec-jsonl"
import { fsFileSink, fsFileSource, gzipFileSink, gzipFileSource, readableSource, writableSink } from "./index"

const workspace = join(tmpdir(), `io-framework-${process.pid}`)

describe("node file pipeline", () => {
  afterEach(async () => {
    await rm(workspace, { recursive: true, force: true })
  })

  it("converts CSV files to JSONL files", async () => {
    await mkdir(workspace, { recursive: true })
    const input = join(workspace, "users.csv")
    const output = join(workspace, "users.jsonl")
    await writeFile(input, "id,name\n1, Ada \n2,Linus\n")

    const progress = progressBehavior()
    const result = await pipeline()
      .from(fsFileSource(input))
      .through(csvDecoder({ header: true }))
      .through(
        map((row: Record<string, string>) => ({
          id: row.id,
          name: row.name.trim()
        }))
      )
      .through(jsonlEncoder())
      .batch({ size: 1 })
      .to(fsFileSink(output, { createParentDirectories: true }))
      .run({ runtime: "node", behaviors: [progress] })

    await expect(readFile(output, "utf8")).resolves.toBe('{"id":"1","name":"Ada"}\n{"id":"2","name":"Linus"}\n')
    expect(result.ok).toBe(true)
    expect(progress.getSnapshot().recordsRead).toBeGreaterThan(0)
    expect(progress.getSnapshot().batchesWritten).toBe(2)
  })

  it("commits atomic file writes only after a successful pipeline", async () => {
    await mkdir(workspace, { recursive: true })
    const output = join(workspace, "atomic.txt")
    const encoder = new TextEncoder()

    const result = await pipeline()
      .from({
        kind: "source",
        name: "bytes",
        async *read() {
          yield encoder.encode("complete")
        }
      })
      .to(fsFileSink(output, { atomic: true, createParentDirectories: true }))
      .run({ runtime: "node" })

    expect(result.ok).toBe(true)
    await expect(readFile(output, "utf8")).resolves.toBe("complete")
  })

  it("keeps the previous target when an atomic write fails", async () => {
    await mkdir(workspace, { recursive: true })
    const output = join(workspace, "atomic-fail.txt")
    const encoder = new TextEncoder()
    await writeFile(output, "original")

    const result = await pipeline()
      .from({
        kind: "source",
        name: "failing-bytes",
        async *read() {
          yield encoder.encode("partial")
          throw new Error("source failed")
        }
      })
      .batch({ size: 1 })
      .to(fsFileSink(output, { atomic: true }))
      .run({ runtime: "node" })

    expect(result.ok).toBe(false)
    await expect(readFile(output, "utf8")).resolves.toBe("original")
  })

  it("writes and reads gzip files", async () => {
    await mkdir(workspace, { recursive: true })
    const output = join(workspace, "records.jsonl.gz")
    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    const chunks: Uint8Array[] = []

    const writeResult = await pipeline()
      .from({
        kind: "source",
        name: "bytes",
        async *read() {
          yield encoder.encode('{"id":1}\n{"id":2}\n')
        }
      })
      .to(gzipFileSink(output, { atomic: true, createParentDirectories: true }))
      .run({ runtime: "node" })

    const readResult = await pipeline()
      .from(gzipFileSource(output))
      .to({
        kind: "sink",
        name: "collector",
        write(batch) {
          chunks.push(...batch.items)
        }
      })
      .run({ runtime: "node" })

    expect(writeResult.ok).toBe(true)
    expect(readResult.ok).toBe(true)
    expect(chunks.map((chunk) => decoder.decode(chunk)).join("")).toBe('{"id":1}\n{"id":2}\n')
  })

  it("emits byte metadata for fs source and sink", async () => {
    await mkdir(workspace, { recursive: true })
    const input = join(workspace, "bytes-in.txt")
    const output = join(workspace, "bytes-out.txt")
    await writeFile(input, "hello")
    const byteEvents: Array<{ stage: string; action: string; bytes: unknown }> = []

    const result = await pipeline()
      .from(fsFileSource(input))
      .to(fsFileSink(output, { createParentDirectories: true }))
      .run({
        runtime: "node",
        onEvent(event) {
          if (event.type === "record" && typeof event.metadata?.bytes === "number") {
            byteEvents.push({ stage: event.stage, action: event.action, bytes: event.metadata.bytes })
          }
        }
      })

    expect(result.ok).toBe(true)
    expect(byteEvents).toEqual(
      expect.arrayContaining([
        { stage: "fs-file-source", action: "read", bytes: 5 },
        { stage: "fs-file-sink", action: "written", bytes: 5 }
      ])
    )
  })

  it("reports structured fs source errors", async () => {
    const missing = join(workspace, "missing.csv")

    const result = await pipeline()
      .from(fsFileSource(missing))
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })
      .run({ runtime: "node" })

    expect(result.ok).toBe(false)
    expect(result.errors[0]).toMatchObject({
      code: "FS_SOURCE_ERROR",
      stage: "fs-file-source#1",
      metadata: {
        path: missing,
        nodeCode: "ENOENT",
        stageName: "fs-file-source"
      }
    })
  })

  it("destroys readable streams on cancel while waiting", async () => {
    const readable = new PassThrough()
    const task = pipeline()
      .from(readableSource(readable))
      .to({
        kind: "sink",
        name: "collector",
        write() {}
      })

    const run = task.run({ runtime: "node" })
    await delay(5)
    task.cancel("stop")
    const result = await run

    expect(result.ok).toBe(false)
    expect(readable.destroyed).toBe(true)
  })

  it("destroys writable streams on cancel while writing", async () => {
    const writable = new Writable({
      write(_chunk, _encoding, _callback) {
        // Keep the write pending until cancellation destroys the stream.
      }
    })
    const task = pipeline()
      .from({
        kind: "source",
        name: "bytes",
        async *read() {
          yield new TextEncoder().encode("pending")
        }
      })
      .to(writableSink(writable))

    const run = task.run({ runtime: "node" })
    await delay(5)
    task.cancel("stop")
    const result = await run

    expect(result.ok).toBe(false)
    expect(writable.destroyed).toBe(true)
  })
})

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
