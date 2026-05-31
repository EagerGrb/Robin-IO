# Core API

`@robbin-io/core` is format-agnostic and business-agnostic. It owns the runtime contracts, pipeline builder, execution, batching, cancellation, events, errors, metrics, and behavior hooks.

## Pipeline

```ts
import { pipeline, map } from "@robbin-io/core"

await pipeline()
  .from(source)
  .through(map((record) => record))
  .batch({ size: 1000 })
  .to(sink)
  .run()
```

## Core Contracts

- `Source<T>`: opens and reads an `AsyncIterable<T>`.
- `Sink<T>`: consumes `Batch<T>`.
- `Transform<I, O>`: maps, filters, validates, skips, or expands records.
- `Decoder<T>`: converts `AsyncIterable<Uint8Array>` into records.
- `Encoder<T>`: converts records into `AsyncIterable<Uint8Array>`.
- `RuntimeBehavior`: observes lifecycle, record, batch, error, and generic events.

## Events

Use `run({ onEvent })` for full runtime observation:

```ts
await task.run({
  onEvent(event) {
    console.log(event.type, event.timestamp, event.metadata)
  }
})
```

`onEvent` enables internal event observation, including stage, channel, batch flush, and sink write events. For low-overhead production progress, prefer `progressBehavior()` or custom record/batch behavior hooks.

See `docs/architecture/runtime.md` and `docs/architecture/performance-baseline.md` for the current cost model.

## Errors

`RuntimeError` includes:

- `code`
- `stage`
- `input`
- `metadata`
- `cause`

Format-specific location details should live in `metadata`, not in core fields.

Core exports `CORE_RUNTIME_ERROR_CODES` as the stable error-code registry for framework-level failures. Adapters and codecs may still use package-specific string codes, but should prefer the shared names when the failure maps to a core category.

## Type Inference

The public pipeline builder carries the current item type across source, transform, plugin, decoder, encoder, batch, and sink boundaries:

```ts
import { map, pipeline, type PipelinePlugin } from "@robbin-io/core"

const normalizeUser: PipelinePlugin<{ id: number }, { id: string }> = {
  name: "normalize-user",
  configure(builder) {
    return builder.through(map((row) => ({ id: String(row.id) })))
  }
}

await pipeline().from(source).use(normalizeUser).to(sink).run()
```

`Decoder<T>` stages are typed for `Uint8Array` streams. `Encoder<T>` stages return `Uint8Array` streams. `npm run typecheck:api` protects these public inference contracts and the root/internal API boundary.

## Internal Boundary

The root `@robbin-io/core` entry is the stable public API surface. It intentionally exports user-facing builders, contracts, behaviors, transforms, errors, and metrics collectors.

Execution helpers such as runtime state, runner functions, channel implementation, scheduler internals, and event factory helpers live under the explicit internal entry:

```ts
import { createRuntimeState } from "@robbin-io/core/internal"
```

`@robbin-io/core/internal` is available for framework-owned packages and advanced beta experiments, but it is not covered by the same compatibility promise as the root entry. Prefer the root entry for application code.
