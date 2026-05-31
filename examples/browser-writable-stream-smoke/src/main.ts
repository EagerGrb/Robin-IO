import { writableStreamSink } from "@robbin-io/browser"
import { map, pipeline } from "@robbin-io/core"

interface WritableStreamSmokeResult {
  readonly ok: boolean
  readonly text: string
  readonly bytesWritten: number
  readonly chunksWritten: number
  readonly closed: boolean
  readonly abortCode?: string
  readonly errors: string[]
}

const output = document.querySelector<HTMLPreElement>("#result")

void runSmoke()
  .then((result) => writeResult(result))
  .catch((error: unknown) =>
    writeResult({
      ok: false,
      text: "",
      bytesWritten: 0,
      chunksWritten: 0,
      closed: false,
      errors: [error instanceof Error ? error.message : String(error)]
    })
  )

async function runSmoke(): Promise<WritableStreamSmokeResult> {
  const success = await runSuccessPath()
  const failure = await runFailurePath()
  return {
    ok: success.ok && failure.ok,
    text: success.text,
    bytesWritten: success.bytesWritten,
    chunksWritten: success.chunksWritten,
    closed: success.closed,
    abortCode: failure.abortCode,
    errors: [...success.errors, ...failure.errors]
  }
}

async function runSuccessPath(): Promise<WritableStreamSmokeResult> {
  const chunks: Array<Uint8Array | string> = []
  let closed = false
  const stream = new WritableStream<Uint8Array | string>({
    write(chunk) {
      chunks.push(chunk)
    },
    close() {
      closed = true
    }
  })
  const sink = writableStreamSink<Uint8Array | string>(stream, { name: "browser-stream-export" })
  const result = await pipeline()
    .from({
      kind: "source",
      name: "chunks",
      async *read() {
        yield "hello"
        yield new TextEncoder().encode(" stream")
      }
    })
    .to(sink)
    .run({ runtime: "browser" })

  const text = chunks.map((chunk) => (typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk))).join("")
  const ok =
    result.ok && text === "hello stream" && closed && sink.getBytesWritten() === 12 && sink.getChunksWritten() === 2

  return {
    ok,
    text,
    bytesWritten: sink.getBytesWritten(),
    chunksWritten: sink.getChunksWritten(),
    closed,
    errors: result.errors.map((error) => `${error.code}:${error.message}`)
  }
}

async function runFailurePath(): Promise<{
  readonly ok: boolean
  readonly abortCode?: string
  readonly errors: string[]
}> {
  let abortReason: { readonly code?: string } | undefined
  const stream = new WritableStream<string>({
    abort(reason) {
      abortReason = reason as { readonly code?: string }
    }
  })
  const result = await pipeline()
    .from({
      kind: "source",
      name: "records",
      async *read() {
        yield "bad"
      }
    })
    .through(
      map((item: string) => {
        throw new Error(`cannot export ${item}`)
      })
    )
    .to(writableStreamSink(stream, { name: "browser-stream-export-failure" }))
    .run({ runtime: "browser" })

  return {
    ok: !result.ok && abortReason?.code === "WRITABLE_STREAM_ABORTED",
    abortCode: abortReason?.code,
    errors: result.errors.map((error) => `${error.code}:${error.message}`)
  }
}

function writeResult(result: WritableStreamSmokeResult): void {
  if (output) {
    output.textContent = JSON.stringify(result)
  }
  ;(
    globalThis as typeof globalThis & { __IO_BROWSER_WRITABLE_STREAM_SMOKE__?: WritableStreamSmokeResult }
  ).__IO_BROWSER_WRITABLE_STREAM_SMOKE__ = result
}
