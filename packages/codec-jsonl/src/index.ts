import { RuntimeError, type Decoder, type Encoder } from "@robbin-io/core"

export interface JsonlDecoderOptions {
  readonly ignoreEmptyLines?: boolean
  readonly maxLineBytes?: number
}

export interface JsonlEncoderOptions {
  readonly chunkBytes?: number
}

export function jsonlDecoder<T = unknown>(options: JsonlDecoderOptions = {}): Decoder<T> {
  const ignoreEmptyLines = options.ignoreEmptyLines ?? true
  const maxLineBytes = options.maxLineBytes

  return {
    kind: "decoder",
    name: "jsonl-decoder",
    async *decode(input) {
      const decoder = new TextDecoder()
      let buffer = ""
      let lineNumber = 1

      for await (const chunk of input) {
        buffer += decoder.decode(chunk, { stream: true })
        const lines = buffer.split(/\r?\n/)
        buffer = lines.pop() ?? ""
        assertLineBytes(buffer, maxLineBytes, lineNumber + lines.length)

        for (const line of lines) {
          assertLineBytes(line, maxLineBytes, lineNumber)
          if (!(ignoreEmptyLines && line.trim() === "")) {
            yield parseJsonLine<T>(line, lineNumber)
          }
          lineNumber += 1
        }
      }

      buffer += decoder.decode()
      if (!(ignoreEmptyLines && buffer.trim() === "")) {
        assertLineBytes(buffer, maxLineBytes, lineNumber)
        yield parseJsonLine<T>(buffer, lineNumber)
      }
    }
  }
}

export function jsonlEncoder<T = unknown>(options: JsonlEncoderOptions = {}): Encoder<T> {
  const chunkBytes = Math.max(0, options.chunkBytes ?? 0)

  return {
    kind: "encoder",
    name: "jsonl-encoder",
    async *encode(input) {
      const encoder = new TextEncoder()
      if (chunkBytes > 0) {
        let buffer = ""
        let bufferBytes = 0
        for await (const item of input) {
          const line = `${JSON.stringify(item)}\n`
          buffer += line
          bufferBytes += utf8ByteLength(line)
          if (bufferBytes >= chunkBytes) {
            yield encoder.encode(buffer)
            buffer = ""
            bufferBytes = 0
          }
        }
        if (buffer.length > 0) {
          yield encoder.encode(buffer)
        }
        return
      }

      for await (const item of input) {
        yield encoder.encode(`${JSON.stringify(item)}\n`)
      }
    }
  }
}

function parseJsonLine<T>(line: string, lineNumber: number): T {
  try {
    return JSON.parse(line) as T
  } catch (error) {
    throw new RuntimeError(`Invalid JSONL at line ${lineNumber}`, {
      code: "JSONL_DECODE_ERROR",
      cause: error,
      metadata: {
        line: lineNumber,
        raw: truncateRaw(line)
      }
    })
  }
}

function assertLineBytes(line: string, maxLineBytes: number | undefined, lineNumber: number): void {
  if (maxLineBytes === undefined) {
    return
  }

  const bytes = new TextEncoder().encode(line).byteLength
  if (bytes > maxLineBytes) {
    throw new RuntimeError(`JSONL line ${lineNumber} exceeds maxLineBytes`, {
      code: "JSONL_LINE_TOO_LONG",
      metadata: {
        line: lineNumber,
        bytes,
        maxLineBytes,
        raw: truncateRaw(line)
      }
    })
  }
}

function truncateRaw(line: string): string {
  return line.length > 200 ? `${line.slice(0, 200)}...` : line
}

function utf8ByteLength(value: string): number {
  let bytes = 0
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code <= 0x7f) {
      bytes += 1
    } else if (code <= 0x7ff) {
      bytes += 2
    } else if (code >= 0xd800 && code <= 0xdbff && index + 1 < value.length) {
      const next = value.charCodeAt(index + 1)
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4
        index += 1
      } else {
        bytes += 3
      }
    } else {
      bytes += 3
    }
  }
  return bytes
}
