import { pipeline, type Sink, type Source, type Transform } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"
import { memorySource } from "@robbin-io/source-memory"

export async function collectSource<T>(source: Source<T>): Promise<readonly T[]> {
  const sink = memorySink<T>()
  const result = await pipeline().from(source).to(sink).run()
  if (!result.ok) {
    throw result.errors[0]
  }
  return sink.getItems()
}

export async function runTransform<I, O>(input: readonly I[], transform: Transform<I, O>): Promise<readonly O[]> {
  const sink = memorySink<O>()
  const result = await pipeline().from(memorySource(input)).through(transform).to(sink).run()
  if (!result.ok) {
    throw result.errors[0]
  }
  return sink.getItems()
}

export async function assertSinkAccepts<T>(sink: Sink<T>, items: readonly T[]): Promise<void> {
  const result = await pipeline().from(memorySource(items)).to(sink).run()
  if (!result.ok) {
    throw result.errors[0]
  }
}
