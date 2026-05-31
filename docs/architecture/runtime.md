# Runtime Architecture

The runtime is intentionally linear:

```text
Source<Uint8Array | T>
  -> Decoder<T>?
  -> Transform<T, U>*
  -> Encoder<U>?
  -> Batch<U>
  -> Sink<U>
```

The core package never imports Node or browser APIs. Platform-specific concerns live in adapter packages and expose the same `Source` and `Sink` contracts.

The core package also never depends on CSV, JSONL, Excel, Parquet, databases, or object storage. Those are format or connector plugins layered on top of the runtime contracts.

## Plugins

Pipeline plugins are a lightweight composition layer around the same core primitives. A plugin can configure transforms, codecs, behaviors, or defaults without adding format-specific assumptions to the core.

## Dead Letters

Dead-letter output should be serializable and format-agnostic. The core exposes `DeadLetterRecord` so behaviors and sinks can write errors to JSONL, databases, or remote error collectors without depending on a specific input format.

## Web Workers

Browser-heavy transforms can run through `webWorkerTransform()` from `@robbin-io/browser`. The core only sees a normal `Transform`; the browser adapter owns the Worker protocol and lifecycle.

## Behavior Hooks

Progress, error reporting, and lifecycle observation are implemented as `RuntimeBehavior` plugins. The executor emits record, batch, error, start, and finish events; behaviors can observe those events without changing the business transform API.

## Cancellation

Cancellation is unified through `AbortSignal`. `RunOptions.signal` is bridged into the runtime context, and behaviors can call `ctx.abort(reason)` to stop long-running work.

## Batching

Batching is a public pipeline stage, not a hidden sink detail. The current implementation supports `size`, `maxBytes`, `estimateBytes`, and `maxWaitMs`.

## Internal Runtime Kernel

The runtime uses small internal protocols behind the unchanged public pipeline API:

- `RuntimeChannel<T>` is a bounded queue between stages. It owns push/pull waiting, close, failure, cancellation release, size reporting, and channel metrics.
- `InternalStage` is a stable description of source, decoder, transform, encoder, batch, and sink work. The scheduler uses `name#n` ids so repeated names remain observable.
- Event helpers in `events.ts` create stage, channel, sink write, and batch flush events. These events are dispatched to metrics, `RunOptions.onEvent`, and behavior `onEvent` hooks.
- Metrics map internal events to names such as `stage.map#1.item.totalMs`, `channel.source-map.size`, `sink.writer#1.write.totalMs`, and `batch.batch#1.flushes`.
- Transform, decoder, encoder, batch, and sink work are runner-backed. Codec logic still lives in codec packages; core only owns the runner protocol.
- Node stream adapters participate in the same cancellation and event model for byte-level read/write metadata.

The current public pipeline API is unchanged. The remaining production work is mainly performance optimization, richer adapter/codec metadata, browser large-file behavior, and public/internal export hardening.

## Observability And Performance Modes

The runtime automatically chooses the cheapest execution path that preserves the requested semantics. There is no public mode flag yet; the mode follows from `RunOptions` and behavior hooks.

### Default Fast Path

When there is no `onEvent` callback and no behavior event hook, the runtime uses summary-style internal observation:

- stage, record, batch, sink, and channel metrics are still updated;
- high-frequency channel push/pull events are not materialized for observers;
- simple source, transform, codec, and batch/sink paths can use direct execution helpers;
- this is the default high-throughput production path.

The current P2.8 baseline for this mode is approximately 46ms for `transform: map 10k records` and 520ms for `pipeline: map+batch+memory 100k records`.

### Behavior-Only Progress Path

Behaviors such as `progressBehavior()` can observe record and batch notifications without requiring full internal channel events. This path is intended for user-facing progress reporting where throughput still matters.

The current P2.8 baseline for `behavior: progress map 10k records` is approximately 58-60ms.

### Full Observer Path

When `run({ onEvent })` is provided, or a behavior implements `onEvent`, the runtime preserves complete internal event visibility:

- source, transform, codec, batch, and sink stage events;
- per-item stage events;
- channel push, pull, wait, and backpressure events;
- batch flush and sink write events;
- record, batch, lifecycle, and error events.

This path is intended for diagnostics, tracing, audit, and production incident analysis. It is deliberately more expensive than the default fast path because event semantics are complete. The current P2.8 baseline for `observer: map 10k records full events` is approximately 225-235ms.

## Guardrails

Performance work should prefer shared runtime and metrics hot-path improvements over new special-purpose executors. A separate unordered concurrent transform direct executor was evaluated during P2.8 and rejected because the measured gain was too small for the maintenance cost.

For numeric baselines and regression guardrails, see `docs/architecture/performance-baseline.md`.
