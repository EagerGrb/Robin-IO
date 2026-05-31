import type { Sink } from "@robbin-io/core"

export type FileSinkTarget = string | URL | "download"

export async function fileSink(
  target: FileSinkTarget,
  mimeType = "application/octet-stream"
): Promise<Sink<Uint8Array | string>> {
  if (target === "download") {
    const { downloadSink } = await import("@robbin-io/browser")
    return downloadSink(mimeType)
  }

  const { fsFileSink } = await import("@robbin-io/node")
  return fsFileSink(target, { createParentDirectories: true })
}

export { downloadSink, writableStreamSink } from "@robbin-io/browser"
export { fsFileSink } from "@robbin-io/node"
