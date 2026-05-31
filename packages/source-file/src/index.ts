import type { Source } from "@robbin-io/core"

export type FileLikeInput = string | URL | Blob

export async function fileSource(input: FileLikeInput): Promise<Source<Uint8Array>> {
  if (typeof Blob !== "undefined" && input instanceof Blob) {
    const { blobSource } = await import("@robbin-io/browser")
    return blobSource(input)
  }

  const { fsFileSource } = await import("@robbin-io/node")
  return fsFileSource(input as string | URL)
}

export { blobSource, fileSource as browserFileSource } from "@robbin-io/browser"
export { fsFileSource } from "@robbin-io/node"
