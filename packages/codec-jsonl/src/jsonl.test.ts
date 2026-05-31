import { describe, expect, it } from "vitest"
import fc from "fast-check"
import { pipeline } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"
import { memorySource } from "@robbin-io/source-memory"
import { jsonlDecoder, jsonlEncoder } from "./index"

describe("jsonl codec", () => {
  it("decodes JSONL chunks", async () => {
    const sink = memorySink<{ id: number }>()
    const encoder = new TextEncoder()

    const result = await pipeline()
      .from(memorySource([encoder.encode('{"id":1}\n{"id":2}\n')]))
      .through(jsonlDecoder<{ id: number }>())
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(sink.getItems()).toEqual([{ id: 1 }, { id: 2 }])
    expect(result.metrics["channel.jsonl-decoder#1:input.push"]).toBeUndefined()
    expect(result.metrics["channel.jsonl-decoder#1:output.pull"]).toBeUndefined()
  })

  it("encodes records as newline-delimited JSON", async () => {
    const sink = memorySink<Uint8Array>()
    const decoder = new TextDecoder()

    const result = await pipeline()
      .from(memorySource([{ id: 1 }, { id: 2 }]))
      .through(jsonlEncoder())
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(
      sink
        .getItems()
        .map((chunk) => decoder.decode(chunk))
        .join("")
    ).toBe('{"id":1}\n{"id":2}\n')
    expect(result.metrics["channel.jsonl-encoder#1:input.push"]).toBeUndefined()
    expect(result.metrics["channel.jsonl-encoder#1:output.pull"]).toBeUndefined()
  })

  it("coalesces encoded JSONL chunks without changing output", async () => {
    const values = Array.from({ length: 100 }, (_, id) => ({ id, name: `User ${id}` }))
    const baseline = await encodeJsonlValues(values)
    const sink = memorySink<Uint8Array>()

    const result = await pipeline()
      .from(memorySource(values))
      .through(jsonlEncoder({ chunkBytes: 128 }))
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(concatBytes(sink.getItems())).toEqual(baseline)
    expect(sink.getItems().length).toBeLessThan(values.length)
  })

  it("reports parse errors with line metadata and stable codec stage id", async () => {
    const sink = memorySink<{ id: number }>()
    const encoder = new TextEncoder()

    const result = await pipeline()
      .from(memorySource([encoder.encode('{"id":1}\n{"id":')]))
      .through(jsonlDecoder<{ id: number }>())
      .to(sink)
      .run()

    expect(result.ok).toBe(false)
    expect(result.errors[0]).toMatchObject({
      code: "JSONL_DECODE_ERROR",
      stage: "jsonl-decoder#1",
      metadata: {
        line: 2,
        raw: '{"id":',
        stageName: "jsonl-decoder"
      }
    })
  })

  it("protects against oversized JSONL lines", async () => {
    const sink = memorySink<{ id: number }>()
    const encoder = new TextEncoder()

    const result = await pipeline()
      .from(memorySource([encoder.encode('{"id":123}\n')]))
      .through(jsonlDecoder<{ id: number }>({ maxLineBytes: 4 }))
      .to(sink)
      .run()

    expect(result.ok).toBe(false)
    expect(result.errors[0]).toMatchObject({
      code: "JSONL_LINE_TOO_LONG",
      stage: "jsonl-decoder#1",
      metadata: {
        line: 1,
        maxLineBytes: 4,
        stageName: "jsonl-decoder"
      }
    })
  })

  it("round-trips JSON values across JSONL encoder and decoder", async () => {
    await fc.assert(
      fc.asyncProperty(jsonValuesArbitrary(), async (values) => {
        const encoded = await encodeJsonlValues(values)
        const decoded = await decodeJsonlValues([encoded])

        expect(toJsonComparable(decoded)).toEqual(toJsonComparable(values))
      }),
      { numRuns: 50 }
    )
  })

  it("decodes JSONL across arbitrary chunk boundaries", async () => {
    await fc.assert(
      fc.asyncProperty(
        jsonValuesArbitrary(),
        fc.array(fc.integer({ min: 1, max: 9 }), { minLength: 1 }),
        async (values, chunkSizes) => {
          const encoded = await encodeJsonlValues(values)
          const whole = await decodeJsonlValues([encoded])
          const chunked = await decodeJsonlValues(splitBytes(encoded, chunkSizes))

          expect(chunked).toEqual(whole)
        }
      ),
      { numRuns: 50 }
    )
  })
})

type JsonPrimitive = string | number | boolean | null
type JsonValue = JsonPrimitive | JsonValue[] | { readonly [key: string]: JsonValue }

function jsonValuesArbitrary(): fc.Arbitrary<readonly JsonValue[]> {
  return fc.array(fc.jsonValue(), { minLength: 1, maxLength: 20 }) as fc.Arbitrary<readonly JsonValue[]>
}

async function encodeJsonlValues(values: readonly JsonValue[]): Promise<Uint8Array> {
  const sink = memorySink<Uint8Array>()
  const result = await pipeline().from(memorySource(values)).through(jsonlEncoder()).to(sink).run()

  expect(result.ok).toBe(true)
  return concatBytes(sink.getItems())
}

async function decodeJsonlValues(chunks: readonly Uint8Array[]): Promise<unknown[]> {
  const sink = memorySink()
  const result = await pipeline().from(memorySource(chunks)).through(jsonlDecoder()).to(sink).run()

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

function toJsonComparable(value: unknown): unknown {
  return JSON.parse(JSON.stringify(value))
}
