import { CORE_RUNTIME_ERROR_CODES, RuntimeError, type Decoder, type Encoder } from "@robbin-io/core"

export type CsvRow = string[] | Record<string, string>

export interface CsvDecoderOptions {
  readonly header?: boolean
  readonly delimiter?: string
  readonly skipEmptyLines?: boolean
  readonly browserChunkSize?: number
}

export interface CsvEncoderOptions {
  readonly header?: readonly string[]
  readonly delimiter?: string
  readonly escapeFormula?: boolean
  readonly chunkBytes?: number
}

export function csvDecoder(options: CsvDecoderOptions = {}): Decoder<CsvRow> {
  return {
    kind: "decoder",
    name: "csv-decoder",
    async *decode(input) {
      if (isNodeRuntime()) {
        yield* parseWithCsvParseStream(input, options)
        return
      }

      yield* parseWithPapaParseChunks(input, options)
    }
  }
}

export function csvEncoder<T extends Record<string, unknown> | readonly unknown[]>(
  options: CsvEncoderOptions = {}
): Encoder<T> {
  const delimiter = options.delimiter ?? ","
  const escapeFormula = options.escapeFormula ?? false
  const chunkBytes = Math.max(0, options.chunkBytes ?? 0)

  return {
    kind: "encoder",
    name: "csv-encoder",
    async *encode(input) {
      const encoder = new TextEncoder()
      let headerWritten = false
      let header = options.header ? [...options.header] : undefined
      let buffer = ""
      let bufferBytes = 0

      const appendLine = function* (line: string): Generator<Uint8Array> {
        if (chunkBytes <= 0) {
          yield encoder.encode(line)
          return
        }

        buffer += line
        bufferBytes += utf8ByteLength(line)
        if (bufferBytes >= chunkBytes) {
          yield encoder.encode(buffer)
          buffer = ""
          bufferBytes = 0
        }
      }

      for await (const item of input) {
        if (Array.isArray(item)) {
          yield* appendLine(`${item.map((value) => escapeCsvValue(value, escapeFormula)).join(delimiter)}\n`)
          continue
        }

        const record = item as Record<string, unknown>
        header ??= Object.keys(record)
        if (!headerWritten) {
          yield* appendLine(`${header.map((value) => escapeCsvValue(value, false)).join(delimiter)}\n`)
          headerWritten = true
        }

        const line = header.map((key) => escapeCsvValue(record[key], escapeFormula)).join(delimiter)
        yield* appendLine(`${line}\n`)
      }

      if (buffer.length > 0) {
        yield encoder.encode(buffer)
      }
    }
  }
}

async function* parseWithCsvParseStream(
  input: AsyncIterable<Uint8Array>,
  options: CsvDecoderOptions
): AsyncIterable<CsvRow> {
  const csvParseSpecifier = "csv-parse"
  const { parse } = (await import(/* @vite-ignore */ csvParseSpecifier)) as typeof import("csv-parse")
  const parser = parse({
    columns: options.header ?? false,
    delimiter: options.delimiter ?? ",",
    skip_empty_lines: options.skipEmptyLines ?? true
  })

  const pump = (async () => {
    try {
      for await (const chunk of input) {
        if (!parser.write(chunk)) {
          await waitForDrain(parser)
        }
      }
      parser.end()
    } catch (error) {
      parser.destroy(error instanceof Error ? error : new Error(String(error)))
    }
  })()

  try {
    try {
      for await (const row of parser) {
        yield row as CsvRow
      }
      await pump
    } catch (error) {
      throw toCsvDecodeError(error)
    }
  } finally {
    parser.destroy()
  }
}

function waitForDrain(stream: {
  off(event: "drain", listener: () => void): unknown
  off(event: "error", listener: (error: Error) => void): unknown
  once(event: "drain", listener: () => void): unknown
  once(event: "error", listener: (error: Error) => void): unknown
}): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      stream.off("drain", onDrain)
      stream.off("error", onError)
    }
    const onDrain = () => {
      cleanup()
      resolve()
    }
    const onError = (error: Error) => {
      cleanup()
      reject(error)
    }

    stream.once("drain", onDrain)
    stream.once("error", onError)
  })
}

async function* parseWithPapaParseChunks(
  input: AsyncIterable<Uint8Array>,
  options: CsvDecoderOptions
): AsyncIterable<CsvRow> {
  const module = await import("papaparse")
  const Papa = module.default
  const handle = new Papa.ParserHandle({
    header: options.header ?? false,
    delimiter: options.delimiter ?? "",
    skipEmptyLines: options.skipEmptyLines ?? true,
    dynamicTyping: false,
    transform: false
  })
  const decoder = new TextDecoder()
  let partial = ""
  let baseIndex = 0

  for await (const chunk of input) {
    const aggregate = partial + decoder.decode(chunk, { stream: true })
    const result = handle.parse(aggregate, baseIndex, true)
    yield* drainPapaResult(result)
    const cursor = result.meta.cursor
    partial = aggregate.substring(cursor - baseIndex)
    baseIndex = cursor
  }

  const result = handle.parse(partial + decoder.decode(), baseIndex, false)
  yield* drainPapaResult(result)
}

function* drainPapaResult(result: PapaParseResultLike): Iterable<CsvRow> {
  if (result.errors.length > 0) {
    throw toPapaParseError(result.errors)
  }

  for (const row of result.data) {
    yield row as CsvRow
  }
}

function escapeCsvValue(value: unknown, escapeFormula: boolean): string {
  const text = escapeFormula ? escapeFormulaValue(value) : value === null || value === undefined ? "" : String(value)
  if (/[",\r\n]/.test(text)) {
    return `"${text.replaceAll('"', '""')}"`
  }
  return text
}

function escapeFormulaValue(value: unknown): string {
  const text = value === null || value === undefined ? "" : String(value)
  return /^[=+\-@\t\r]/.test(text) ? `'${text}` : text
}

interface CsvParseErrorLike {
  readonly message?: string
  readonly code?: string
  readonly lines?: number
  readonly records?: number
  readonly column?: string | number
  readonly index?: number
  readonly invalid_field_length?: number
  readonly quoting?: boolean
}

interface PapaParseErrorLike {
  readonly message?: string
  readonly type?: string
  readonly code?: string
  readonly row?: number
  readonly index?: number
}

interface PapaParseResultLike {
  readonly data: unknown[]
  readonly errors: readonly PapaParseErrorLike[]
  readonly meta: {
    readonly cursor: number
  }
}

interface PapaParserHandle {
  parse(input: string, baseIndex: number, ignoreLastRow: boolean): PapaParseResultLike
}

interface PapaParseModule {
  readonly ParserHandle: new (config: Record<string, unknown>) => PapaParserHandle
}

function toCsvDecodeError(error: unknown): RuntimeError {
  if (error instanceof RuntimeError) {
    return error
  }

  const csvError = error as CsvParseErrorLike
  return new RuntimeError(csvError.message ?? "CSV decode failed", {
    code: CORE_RUNTIME_ERROR_CODES.codecDecodeError,
    cause: error,
    metadata: compactMetadata({
      format: "csv",
      parser: "csv-parse",
      parserCode: csvError.code,
      line: csvError.lines,
      row: csvError.records,
      column: csvError.column,
      index: csvError.index,
      invalidFieldLength: csvError.invalid_field_length,
      quoting: csvError.quoting
    })
  })
}

function toPapaParseError(errors: readonly PapaParseErrorLike[]): RuntimeError {
  const first = errors[0]
  return new RuntimeError(first?.message ?? "CSV decode failed", {
    code: CORE_RUNTIME_ERROR_CODES.codecDecodeError,
    metadata: compactMetadata({
      format: "csv",
      parser: "papaparse",
      parserCode: first?.code,
      parserType: first?.type,
      row: first?.row,
      index: first?.index,
      errors: errors.map((error) =>
        compactMetadata({
          message: error.message,
          type: error.type,
          code: error.code,
          row: error.row,
          index: error.index
        })
      )
    })
  })
}

function compactMetadata(input: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined))
}

function isNodeRuntime(): boolean {
  const maybeGlobal = globalThis as typeof globalThis & { process?: { versions?: { node?: string } } }
  return typeof maybeGlobal.process?.versions?.node === "string"
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
