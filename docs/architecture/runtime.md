# Runtime Architecture / 运行时架�?
The MVP runtime is intentionally linear:

MVP 运行时有意保持线性结构：

```text
Source<Uint8Array | T>
  -> Decoder<T>?
  -> Transform<T, U>*
  -> Encoder<U>?
  -> Batch<U>
  -> Sink<U>
```

The core package never imports Node or browser APIs. Platform-specific concerns live in adapter packages and expose the same `Source` and `Sink` contracts.

核心 package 不直接导�?Node 或浏览器 API。平台相关能力放在适配 package 中，并统一暴露相同�?`Source` �?`Sink` 契约�?
The core package also never depends on CSV, JSONL, Excel, Parquet, databases, or object storage. Those are format or connector plugins layered on top of the runtime contracts.

核心 package 也不依赖 CSV、JSONL、Excel、Parquet、数据库或对象存储。这些都属于构建在运行时契约之上的格式插件或连接器插件�?
## Plugins / 插件

Pipeline plugins are a lightweight composition layer around the same core primitives. A plugin can configure transforms, codecs, behaviors, or defaults without adding format-specific assumptions to the core.

Pipeline 插件是围绕核心基础能力的一层轻量组合机制。插件可以配�?transform、codec、behavior 或默认参数，但不会把具体格式的假设放�?core�?
## Dead Letters / 死信记录

Dead-letter output should be serializable and format-agnostic. The core exposes `DeadLetterRecord` so behaviors and sinks can write errors to JSONL, databases, or remote error collectors without depending on a specific input format.

死信输出应该是可序列化且格式无关的。core 暴露 `DeadLetterRecord`，让行为�?sink 能把错误写入 JSONL、数据库或远程错误收集系统，而不依赖某一种输入格式�?
## Web Workers / Web Worker

Browser-heavy transforms can run through `webWorkerTransform()` from `@robbin-io/browser`. The core only sees a normal `Transform`; the browser adapter owns the Worker protocol and lifecycle.

浏览器中的重计算 transform 可以通过 `@robbin-io/browser` �?`webWorkerTransform()` 执行。core 看到的仍然只是普�?`Transform`；Worker 协议和生命周期由浏览器适配包负责�?
## Behavior Hooks / 行为钩子

Progress, error reporting, and lifecycle observation are implemented as `RuntimeBehavior` plugins. The executor emits record, batch, error, start, and finish events; behaviors can observe those events without changing the business transform API.

进度、错误报告和生命周期观察都通过 `RuntimeBehavior` 插件实现。执行器会发�?record、batch、error、start、finish 事件；行为插件可以观察这些事件，而不需要改变业�?transform API�?
## Cancellation / 取消

Cancellation is unified through `AbortSignal`. `RunOptions.signal` is bridged into the runtime context, and behaviors can call `ctx.abort(reason)` to stop long-running work.

取消能力统一通过 `AbortSignal` 实现。`RunOptions.signal` 会桥接到运行时上下文中，行为插件也可以调�?`ctx.abort(reason)` 来停止长时间运行的任务�?
## Batching / 批处�?
Batching is a public pipeline stage, not a hidden sink detail. The current implementation supports `size`, `maxBytes`, `estimateBytes`, and `maxWaitMs`.

## Internal Runtime Kernel

The runtime now uses small internal protocols behind the unchanged public pipeline API:

- `RuntimeChannel<T>` is a bounded queue between stages. It owns push/pull waiting, close, failure, cancellation release, size reporting, and channel metrics.
- `InternalStage` is a stable description of source, decoder, transform, encoder, batch, and sink work. The scheduler uses `name#n` ids so repeated names remain observable.
- Event helpers in `events.ts` create stage, channel, sink write, and batch flush events. These events are dispatched to metrics, `RunOptions.onEvent`, and behavior `onEvent` hooks.
- Metrics map internal events to names such as `stage.map#1.item.totalMs`, `channel.source-map.size`, `sink.writer#1.write.totalMs`, and `batch.batch#1.flushes`.
- Transform, decoder, encoder, batch, and sink work are all runner-backed. Codec logic still lives in codec packages; core only owns the runner protocol.
- Node stream adapters participate in the same cancellation and event model for byte-level read/write metadata.

The current public pipeline API is unchanged. The remaining production work is mainly performance optimization, richer adapter/codec metadata, browser large-file behavior, and public/internal export hardening.

## Observability and Performance Modes

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

- source/transform/codec/batch/sink stage events;
- per-item stage events;
- channel push, pull, wait, and backpressure events;
- batch flush and sink write events;
- record, batch, lifecycle, and error events.

This path is intended for diagnostics, tracing, audit, and production incident analysis. It is deliberately more expensive than the default fast path because event semantics are complete. The current P2.8 baseline for `observer: map 10k records full events` is approximately 225-235ms.

### Guardrails

Performance work should prefer shared runtime and metrics hot-path improvements over new special-purpose executors. A separate unordered concurrent transform direct executor was evaluated during P2.8 and rejected because the measured gain was too small for the maintenance cost.

For the numeric baseline and regression guardrails, see `docs/architecture/performance-baseline.md`.

批处理是公开�?pipeline 阶段，而不是隐藏在 sink 内部的细节。当前实现支�?`size`、`maxBytes`、`estimateBytes` �?`maxWaitMs`�?