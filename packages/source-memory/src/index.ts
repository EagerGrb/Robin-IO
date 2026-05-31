import type { RuntimeContext, Source } from "@robbin-io/core"

export type MemoryInput<T> = Iterable<T> | AsyncIterable<T> | (() => Iterable<T> | AsyncIterable<T>)

export function memorySource<T>(input: MemoryInput<T>, name = "memory-source"): Source<T> {
  return {
    kind: "source",
    name,
    async *read(ctx: RuntimeContext) {
      const resolved = typeof input === "function" ? input() : input
      for await (const item of toAsyncIterable(resolved)) {
        if (ctx.signal.aborted) {
          return
        }
        yield item
      }
    }
  }
}

async function* toAsyncIterable<T>(input: Iterable<T> | AsyncIterable<T>): AsyncIterable<T> {
  if (Symbol.asyncIterator in input) {
    yield* input
    return
  }
  yield* input
}
