import { createReadStream, createWriteStream, type PathLike } from "node:fs"
import { mkdir, rename, rm } from "node:fs/promises"
import { dirname } from "node:path"
import { fileURLToPath } from "node:url"
import { createGunzip, createGzip, type Gzip } from "node:zlib"
import { RuntimeError, type Batch, type RuntimeContext, type Sink, type Source } from "@robbin-io/core"
import type { Readable, Writable } from "node:stream"

export interface FsFileSourceOptions {
  readonly chunkSize?: number
}

export function fsFileSource(path: PathLike, options: FsFileSourceOptions = {}): Source<Uint8Array> {
  const targetPath = normalizePath(path)

  return {
    kind: "source",
    name: "fs-file-source",
    async *read(ctx: RuntimeContext) {
      const stream = createReadStream(path, { highWaterMark: options.chunkSize })
      const cleanupAbort = destroyOnAbort(stream, ctx)
      try {
        for await (const chunk of stream) {
          if (ctx.signal.aborted) {
            stream.destroy()
            return
          }
          const bytes = chunk instanceof Uint8Array ? chunk : Buffer.from(chunk)
          await emitByteEvent(ctx, "fs-file-source", "read", bytes.byteLength)
          yield bytes
        }
      } catch (error) {
        throw toNodeRuntimeError(error, "FS_SOURCE_ERROR", "fs-file-source", targetPath)
      } finally {
        cleanupAbort()
        stream.destroy()
      }
    }
  }
}

export interface FsFileSinkOptions {
  readonly createParentDirectories?: boolean
  readonly atomic?: boolean
}

export function fsFileSink(path: PathLike, options: FsFileSinkOptions = {}): Sink<Uint8Array | string> {
  let stream: ReturnType<typeof createWriteStream> | undefined
  const targetPath = normalizePath(path)
  let activePath = targetPath
  let tempPath: string | undefined

  return {
    kind: "sink",
    name: "fs-file-sink",
    async open() {
      if (options.createParentDirectories) {
        await mkdir(dirname(targetPath), { recursive: true })
      }

      if (options.atomic) {
        tempPath = `${targetPath}.${process.pid}.${Date.now()}.tmp`
        activePath = tempPath
      } else {
        activePath = targetPath
      }

      stream = createWriteStream(activePath)
    },
    async write(batch: Batch<Uint8Array | string>, ctx: RuntimeContext) {
      if (!stream) {
        throw new Error("File sink is not open")
      }

      if (ctx.signal.aborted) {
        return
      }
      const chunk = coalesceBatchItems(batch.items)
      await writeChunk(stream, chunk, ctx)
      await emitByteEvent(ctx, "fs-file-sink", "written", byteLength(chunk))
    },
    async close(ctx: RuntimeContext) {
      if (!stream) {
        return
      }
      const target = stream
      stream = undefined
      await endWritable(target, ctx)

      if (options.atomic && tempPath) {
        if (ctx.signal.aborted || ctx.metadata.get("pipeline.failed") === true) {
          await rm(tempPath, { force: true })
        } else {
          await rename(tempPath, targetPath)
        }
        tempPath = undefined
      }
    }
  }
}

export function gzipFileSource(path: PathLike, options: FsFileSourceOptions = {}): Source<Uint8Array> {
  const targetPath = normalizePath(path)

  return {
    kind: "source",
    name: "gzip-file-source",
    async *read(ctx: RuntimeContext) {
      const stream = createReadStream(path, { highWaterMark: options.chunkSize }).pipe(createGunzip())
      const cleanupAbort = destroyOnAbort(stream, ctx)
      try {
        for await (const chunk of stream) {
          if (ctx.signal.aborted) {
            stream.destroy()
            return
          }
          const bytes = chunk instanceof Uint8Array ? chunk : Buffer.from(chunk)
          await emitByteEvent(ctx, "gzip-file-source", "read", bytes.byteLength)
          yield bytes
        }
      } catch (error) {
        throw toNodeRuntimeError(error, "GZIP_SOURCE_ERROR", "gzip-file-source", targetPath)
      } finally {
        cleanupAbort()
        stream.destroy()
      }
    }
  }
}

export function gzipFileSink(path: PathLike, options: FsFileSinkOptions = {}): Sink<Uint8Array | string> {
  let gzip: Gzip | undefined
  let fileStream: ReturnType<typeof createWriteStream> | undefined
  const targetPath = normalizePath(path)
  let activePath = targetPath
  let tempPath: string | undefined

  return {
    kind: "sink",
    name: "gzip-file-sink",
    async open() {
      if (options.createParentDirectories) {
        await mkdir(dirname(targetPath), { recursive: true })
      }

      if (options.atomic) {
        tempPath = `${targetPath}.${process.pid}.${Date.now()}.tmp`
        activePath = tempPath
      } else {
        activePath = targetPath
      }

      gzip = createGzip()
      fileStream = createWriteStream(activePath)
      gzip.pipe(fileStream)
    },
    async write(batch, ctx) {
      if (!gzip) {
        throw new Error("Gzip file sink is not open")
      }

      if (ctx.signal.aborted) {
        return
      }
      const chunk = coalesceBatchItems(batch.items)
      await writeChunk(gzip, chunk, ctx)
      await emitByteEvent(ctx, "gzip-file-sink", "written", byteLength(chunk))
    },
    async close(ctx) {
      if (!gzip || !fileStream) {
        return
      }

      const activeGzip = gzip
      const activeFileStream = fileStream
      gzip = undefined
      fileStream = undefined
      await finishGzip(activeGzip, activeFileStream)

      if (options.atomic && tempPath) {
        if (ctx.signal.aborted || ctx.metadata.get("pipeline.failed") === true) {
          await rm(tempPath, { force: true })
        } else {
          await rename(tempPath, targetPath)
        }
        tempPath = undefined
      }
    }
  }
}

export function readableSource(readable: Readable, name = "readable-source"): Source<Uint8Array> {
  return {
    kind: "source",
    name,
    async *read(ctx) {
      const cleanupAbort = destroyOnAbort(readable, ctx)
      try {
        for await (const chunk of readable) {
          if (ctx.signal.aborted) {
            readable.destroy()
            return
          }
          const bytes = chunk instanceof Uint8Array ? chunk : Buffer.from(chunk)
          await emitByteEvent(ctx, name, "read", bytes.byteLength)
          yield bytes
        }
      } finally {
        cleanupAbort()
      }
    }
  }
}

export function writableSink(writable: Writable, name = "writable-sink"): Sink<Uint8Array | string> {
  return {
    kind: "sink",
    name,
    async write(batch, ctx) {
      if (ctx.signal.aborted) {
        return
      }
      const chunk = coalesceBatchItems(batch.items)
      await writeChunk(writable, chunk, ctx)
      await emitByteEvent(ctx, name, "written", byteLength(chunk))
    },
    async close(ctx) {
      await endWritable(writable, ctx)
    }
  }
}

function coalesceBatchItems(items: readonly (Uint8Array | string)[]): Uint8Array | string {
  if (items.length === 0) {
    return ""
  }
  if (items.length === 1) {
    return items[0]
  }
  if (items.every((item) => typeof item === "string")) {
    return (items as readonly string[]).join("")
  }
  return Buffer.concat(items.map((item) => (typeof item === "string" ? Buffer.from(item) : Buffer.from(item))))
}

function writeChunk(stream: Writable, chunk: Uint8Array | string, ctx: RuntimeContext): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false
    const cleanup = () => {
      stream.off("drain", onDrain)
      stream.off("error", onError)
      ctx.signal.removeEventListener("abort", onAbort)
    }
    const settle = (error?: Error | RuntimeError | null) => {
      if (settled) {
        return
      }
      settled = true
      cleanup()
      if (error) reject(error)
      else resolve()
    }
    const onDrain = () => settle()
    const onError = (error: Error) => settle(error)
    const onAbort = () => {
      const error = abortError(ctx)
      stream.destroy()
      settle(error)
    }

    if (ctx.signal.aborted) {
      onAbort()
      return
    }

    stream.once("error", onError)
    ctx.signal.addEventListener("abort", onAbort, { once: true })
    if (!stream.write(chunk, (error?: Error | null) => settle(error))) {
      stream.once("drain", onDrain)
    }
  })
}

function endWritable(stream: Writable, ctx: RuntimeContext): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false
    const cleanup = () => {
      stream.off("error", onError)
      ctx.signal.removeEventListener("abort", onAbort)
    }
    const settle = (error?: Error | RuntimeError | null) => {
      if (settled) {
        return
      }
      settled = true
      cleanup()
      if (error) reject(error)
      else resolve()
    }
    const onError = (error: Error) => settle(error)
    const onAbort = () => {
      const error = abortError(ctx)
      stream.destroy()
      settle(error)
    }

    if (ctx.signal.aborted) {
      onAbort()
      return
    }

    stream.once("error", onError)
    ctx.signal.addEventListener("abort", onAbort, { once: true })
    stream.end((error?: Error | null) => settle(error))
  })
}

function finishGzip(gzip: Gzip, fileStream: ReturnType<typeof createWriteStream>): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      gzip.off("error", onError)
      fileStream.off("error", onError)
      fileStream.off("finish", onFinish)
    }
    const onError = (error: Error) => {
      cleanup()
      reject(error)
    }
    const onFinish = () => {
      cleanup()
      resolve()
    }

    gzip.once("error", onError)
    fileStream.once("error", onError)
    fileStream.once("finish", onFinish)
    gzip.end()
  })
}

function normalizePath(path: PathLike): string {
  if (path instanceof URL) {
    return fileURLToPath(path)
  }
  if (Buffer.isBuffer(path)) {
    return path.toString()
  }
  return String(path)
}

function byteLength(item: Uint8Array | string): number {
  return typeof item === "string" ? Buffer.byteLength(item) : item.byteLength
}

async function emitByteEvent(
  ctx: RuntimeContext,
  stage: string,
  action: "read" | "written",
  bytes: number
): Promise<void> {
  await ctx.emit({
    type: "record",
    stage,
    action,
    count: 0,
    timestamp: Date.now(),
    metadata: { bytes }
  })
}

function destroyOnAbort<T extends Readable | Writable>(stream: T, ctx: RuntimeContext): () => void {
  const onAbort = () => stream.destroy()
  if (ctx.signal.aborted) {
    onAbort()
    return () => undefined
  }
  ctx.signal.addEventListener("abort", onAbort, { once: true })
  return () => ctx.signal.removeEventListener("abort", onAbort)
}

function abortError(ctx: RuntimeContext): RuntimeError {
  return new RuntimeError("Pipeline aborted", { code: "PIPELINE_ABORTED", cause: ctx.signal.reason })
}

function toNodeRuntimeError(error: unknown, code: string, stage: string, path: string): RuntimeError {
  if (error instanceof RuntimeError) {
    return error
  }

  const nodeError = error as { code?: unknown; syscall?: unknown }
  return new RuntimeError(error instanceof Error ? error.message : String(error), {
    code,
    stage,
    cause: error,
    metadata: {
      path,
      nodeCode: typeof nodeError.code === "string" ? nodeError.code : undefined,
      syscall: typeof nodeError.syscall === "string" ? nodeError.syscall : undefined
    }
  })
}
