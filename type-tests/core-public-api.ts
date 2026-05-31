import {
  CORE_RUNTIME_ERROR_CODES,
  InMemoryMetricsCollector,
  RuntimeError,
  filter,
  map,
  pipeline,
  transform,
  validate,
  type BatchOptions,
  type Decoder,
  type Encoder,
  type PipelinePlugin,
  type PipelineTask,
  type RuntimeBehavior,
  type RuntimeContext,
  type RunResult,
  type Sink,
  type Source,
  type Transform
} from "@robbin-io/core"
import * as core from "@robbin-io/core"
import { createRuntimeState } from "@robbin-io/core/internal"

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false
type Expect<T extends true> = T

interface InputRow {
  readonly id: number
  readonly name?: string
}

interface OutputRow {
  id: string
  name: string
}

declare const rowsSource: Source<InputRow>
declare const bytesSource: Source<Uint8Array>
declare const inputSink: Sink<InputRow>
declare const outputSink: Sink<OutputRow>
declare const stringSink: Sink<string>
declare const numberSink: Sink<number>
declare const bytesSink: Sink<Uint8Array>

const rowToOutput = map((row: InputRow) => ({
  id: String(row.id),
  name: row.name ?? ""
}))

type _mapPreservesTransformShape = Expect<Equal<typeof rowToOutput, Transform<InputRow, OutputRow>>>

const filteredRows = filter((row: InputRow) => row.id > 0)
type _filterPreservesInputType = Expect<Equal<typeof filteredRows, Transform<InputRow, InputRow>>>

const validatedRows = validate((row: InputRow) => row.id > 0)
type _validatePreservesInputType = Expect<Equal<typeof validatedRows, Transform<InputRow, InputRow>>>

const expandedRows = transform((row: InputRow) => [row, { ...row, id: row.id + 1 }])
type _transformSupportsExpansion = Expect<Equal<typeof expandedRows, Transform<InputRow, InputRow>>>

const stringTask = pipeline()
  .from(rowsSource)
  .through(map((row) => String(row.id)))
  .batch({ size: 128 })
  .to(stringSink)

type _pipelineInfersTaskOutput = Expect<Equal<typeof stringTask, PipelineTask<string>>>

pipeline().from(rowsSource).to(inputSink)
pipeline().from(rowsSource).through(rowToOutput).to(outputSink)

// @ts-expect-error Sink type must match the last pipeline stage output.
pipeline().from(rowsSource).through(rowToOutput).to(numberSink)

const outputPlugin: PipelinePlugin<InputRow, OutputRow> = {
  name: "output-plugin",
  configure(builder) {
    return builder.through(rowToOutput)
  }
}

pipeline().from(rowsSource).use(outputPlugin).to(outputSink)

// @ts-expect-error Plugin output should flow into sink type checking.
pipeline().from(rowsSource).use(outputPlugin).to(inputSink)

const rowDecoder: Decoder<InputRow> = {
  kind: "decoder",
  name: "row-decoder",
  async *decode(_input: AsyncIterable<Uint8Array>, _ctx: RuntimeContext) {
    yield { id: 1 }
  }
}

const rowEncoder: Encoder<InputRow> = {
  kind: "encoder",
  name: "row-encoder",
  async *encode(input: AsyncIterable<InputRow>, _ctx: RuntimeContext) {
    for await (const row of input) {
      yield new TextEncoder().encode(String(row.id))
    }
  }
}

pipeline().from(bytesSource).through(rowDecoder).through(rowEncoder).to(bytesSink)

// @ts-expect-error Decoders consume Uint8Array streams and should not attach to decoded row streams.
pipeline().from(rowsSource).through(rowDecoder)

const behavior: RuntimeBehavior = {
  name: "typed-behavior",
  onRecord(event, ctx) {
    ctx.metrics.increment(`record.${event.action}`, event.count)
  },
  onError(error, ctx) {
    ctx.logger.error?.(error.message, { code: error.code })
  }
}

const batchOptions: BatchOptions<InputRow> = {
  size: 10,
  estimateBytes(row) {
    return row.name?.length ?? 0
  }
}

const metrics = new InMemoryMetricsCollector()
metrics.increment("example.records")
metrics.observe("example.duration", 1)

const runtimeError = new RuntimeError("aborted", {
  code: CORE_RUNTIME_ERROR_CODES.pipelineAborted
})
type _runtimeErrorCodeIsString = Expect<Equal<typeof runtimeError.code, string>>

const state = createRuntimeState({ behaviors: [behavior], metrics })
state.ctx.metadata.set("batchOptions", batchOptions)

declare const result: RunResult
type _runResultOkIsBoolean = Expect<Equal<typeof result.ok, boolean>>

// @ts-expect-error Runtime internals are intentionally not exported from the stable root entry.
core.createRuntimeState
