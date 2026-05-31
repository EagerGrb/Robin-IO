import { describe, expect, it } from "vitest"
import fc from "fast-check"
import { pipeline, type RuntimeEvent } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"
import { memorySource } from "@robbin-io/source-memory"
import { csvDecoder, csvEncoder } from "./index"

describe("csv codec", () => {
  it("decodes header CSV rows", async () => {
    const sink = memorySink()
    const encoder = new TextEncoder()

    const result = await pipeline()
      .from(memorySource([encoder.encode("id,name\n1,Ada\n2,Linus\n")]))
      .through(csvDecoder({ header: true }))
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(sink.getItems()).toEqual([
      { id: "1", name: "Ada" },
      { id: "2", name: "Linus" }
    ])
    expect(result.metrics["channel.csv-decoder#1:input.push"]).toBeUndefined()
    expect(result.metrics["channel.csv-decoder#1:output.pull"]).toBeUndefined()
  })

  it("decodes CSV records across chunk boundaries", async () => {
    const sink = memorySink()
    const encoder = new TextEncoder()

    const result = await pipeline()
      .from(
        memorySource([
          encoder.encode("id,na"),
          encoder.encode('me\n1,"A'),
          encoder.encode('da Lovelace"\n2,Lin'),
          encoder.encode("us\n")
        ])
      )
      .through(csvDecoder({ header: true }))
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(sink.getItems()).toEqual([
      { id: "1", name: "Ada Lovelace" },
      { id: "2", name: "Linus" }
    ])
  })

  it("encodes object rows", async () => {
    const sink = memorySink<Uint8Array>()
    const decoder = new TextDecoder()

    const result = await pipeline()
      .from(memorySource([{ id: 1, name: "Ada" }]))
      .through(csvEncoder())
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(
      sink
        .getItems()
        .map((chunk) => decoder.decode(chunk))
        .join("")
    ).toBe("id,name\n1,Ada\n")
    expect(result.metrics["channel.csv-encoder#1:input.push"]).toBeUndefined()
    expect(result.metrics["channel.csv-encoder#1:output.pull"]).toBeUndefined()
  })

  it("escapes spreadsheet formula values when requested", async () => {
    const sink = memorySink<Uint8Array>()
    const decoder = new TextDecoder()

    const result = await pipeline()
      .from(
        memorySource([
          {
            id: 1,
            name: "=cmd|' /C calc'!A0",
            note: "+SUM(1,2)",
            safe: "Ada"
          }
        ])
      )
      .through(csvEncoder({ escapeFormula: true }))
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(
      sink
        .getItems()
        .map((chunk) => decoder.decode(chunk))
        .join("")
    ).toBe("id,name,note,safe\n1,'=cmd|' /C calc'!A0,\"'+SUM(1,2)\",Ada\n")
  })

  it("keeps formula-looking values unchanged by default", async () => {
    const sink = memorySink<Uint8Array>()
    const decoder = new TextDecoder()

    const result = await pipeline()
      .from(memorySource([{ value: "=1+1" }]))
      .through(csvEncoder())
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(
      sink
        .getItems()
        .map((chunk) => decoder.decode(chunk))
        .join("")
    ).toBe("value\n=1+1\n")
  })

  it("reports CSV parse failures with structured metadata", async () => {
    const sink = memorySink()
    const encoder = new TextEncoder()

    const result = await pipeline()
      .from(memorySource([encoder.encode('id,name\n1,"Ada\n2,Linus\n')]))
      .through(csvDecoder({ header: true }))
      .to(sink)
      .run()

    expect(result.ok).toBe(false)
    expect(result.errors[0]).toMatchObject({
      code: "CODEC_DECODE_ERROR",
      stage: "csv-decoder#1",
      metadata: {
        format: "csv",
        parser: "csv-parse"
      }
    })
    expect(result.errors[0]?.metadata?.parserCode).toEqual(expect.stringContaining("CSV_"))
    expect(typeof result.errors[0]?.metadata?.line).toBe("number")
  })

  it("keeps the channel-backed codec path when full event observation is enabled", async () => {
    const sink = memorySink()
    const encoder = new TextEncoder()
    const events: RuntimeEvent[] = []

    const result = await pipeline()
      .from(memorySource([encoder.encode("id,name\n1,Ada\n")]))
      .through(csvDecoder({ header: true }))
      .to(sink)
      .run({ onEvent: (event) => events.push(event) })

    expect(result.ok).toBe(true)
    expect(
      events.some(
        (event) =>
          (event.type === "channel.push" || event.type === "channel.pull") && event.channel.startsWith("csv-decoder#1:")
      )
    ).toBe(true)
  })

  it("round-trips string rows across CSV encoder and decoder", async () => {
    await fc.assert(
      fc.asyncProperty(csvRowsArbitrary(), async (rows) => {
        const encoded = await encodeCsvRows(rows)
        const decoded = await decodeCsvRows([encoded])

        expect(decoded).toEqual(rows)
      }),
      { numRuns: 50 }
    )
  })

  it("decodes the same CSV text across arbitrary chunk boundaries", async () => {
    await fc.assert(
      fc.asyncProperty(
        csvRowsArbitrary(),
        fc.array(fc.integer({ min: 1, max: 7 }), { minLength: 1 }),
        async (rows, chunkSizes) => {
          const encoded = await encodeCsvRows(rows)
          const whole = await decodeCsvRows([encoded])
          const chunked = await decodeCsvRows(splitBytes(encoded, chunkSizes))

          expect(chunked).toEqual(whole)
        }
      ),
      { numRuns: 50 }
    )
  })

  it("decodes browser CSV chunks without collecting the full text first", async () => {
    const sink = memorySink()
    const encoder = new TextEncoder()

    const result = await withBrowserCsvRuntime(() =>
      pipeline()
        .from(
          memorySource([encoder.encode("id,na"), encoder.encode('me\n1,"A'), encoder.encode('da Lovelace"\n2,Linus\n')])
        )
        .through(csvDecoder({ header: true, browserChunkSize: 4 }))
        .to(sink)
        .run()
    )

    expect(result.ok).toBe(true)
    expect(sink.getItems()).toEqual([
      { id: "1", name: "Ada Lovelace" },
      { id: "2", name: "Linus" }
    ])
  })

  it("reports browser CSV parse failures with PapaParse metadata", async () => {
    const sink = memorySink()
    const encoder = new TextEncoder()

    const result = await withBrowserCsvRuntime(() =>
      pipeline()
        .from(memorySource([encoder.encode('id,name\n1,"Ada\n2,Linus\n')]))
        .through(csvDecoder({ header: true, browserChunkSize: 3 }))
        .to(sink)
        .run()
    )

    expect(result.ok).toBe(false)
    expect(result.errors[0]).toMatchObject({
      code: "CODEC_DECODE_ERROR",
      stage: "csv-decoder#1",
      metadata: {
        format: "csv",
        parser: "papaparse",
        parserCode: "MissingQuotes"
      }
    })
  })
})

function csvRowsArbitrary(): fc.Arbitrary<readonly string[][]> {
  return fc.integer({ min: 1, max: 5 }).chain((columns) =>
    fc.array(fc.array(csvCellArbitrary(), { minLength: columns, maxLength: columns }), {
      minLength: 1,
      maxLength: 12
    })
  )
}

function csvCellArbitrary(): fc.Arbitrary<string> {
  return fc.string({ maxLength: 24 }).filter((value) => !value.includes("\0"))
}

async function encodeCsvRows(rows: readonly string[][]): Promise<Uint8Array> {
  const sink = memorySink<Uint8Array>()
  const result = await pipeline().from(memorySource(rows)).through(csvEncoder()).to(sink).run()

  expect(result.ok).toBe(true)
  return concatBytes(sink.getItems())
}

async function decodeCsvRows(chunks: readonly Uint8Array[]): Promise<unknown[]> {
  const sink = memorySink()
  const result = await pipeline()
    .from(memorySource(chunks))
    .through(csvDecoder({ skipEmptyLines: false }))
    .to(sink)
    .run()

  expect(result.ok).toBe(true)
  return sink.getItems()
}

function splitBytes(input: Uint8Array, chunkSizes: readonly number[]): Uint8Array[] {
  const chunks: Uint8Array[] = []
  let offset = 0
  let index = 0

  while (offset < input.byteLength) {
    const size = chunkSizes[index % chunkSizes.length] ?? 1
    chunks.push(input.slice(offset, offset + size))
    offset += size
    index += 1
  }

  return chunks
}

function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
  const output = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    output.set(chunk, offset)
    offset += chunk.byteLength
  }
  return output
}

async function withBrowserCsvRuntime<T>(callback: () => Promise<T>): Promise<T> {
  const globalWithProcess = globalThis as typeof globalThis & {
    process?: NodeJS.Process & { versions?: NodeJS.ProcessVersions }
  }
  const process = globalWithProcess.process
  const originalVersions = process?.versions

  try {
    if (process) {
      Object.defineProperty(process, "versions", {
        configurable: true,
        value: {}
      })
    }
    return await callback()
  } finally {
    if (process) {
      Object.defineProperty(process, "versions", {
        configurable: true,
        value: originalVersions
      })
    }
  }
}
