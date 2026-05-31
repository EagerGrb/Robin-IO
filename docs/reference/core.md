# Core API / Core API 参�?
`@robbin-io/core` is format-agnostic and business-agnostic. It owns the runtime contracts, pipeline builder, execution, batching, cancellation, events, errors, and behavior hooks.

`@robbin-io/core` 保持格式无关和业务无关。它负责运行时契约、pipeline 构建器、执行、批处理、取消、事件、错误和行为钩子�?
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

## Core Contracts / 核心契约

- `Source<T>`: opens and reads an `AsyncIterable<T>`.
- `Sink<T>`: consumes `Batch<T>`.
- `Transform<I, O>`: maps, filters, validates, or expands records.
- `Decoder<T>`: converts `AsyncIterable<Uint8Array>` into records.
- `Encoder<T>`: converts records into `AsyncIterable<Uint8Array>`.
- `RuntimeBehavior`: observes lifecycle, record, batch, error, and generic events.

## Events / 事件

Use `run({ onEvent })` for generic runtime observation:

使用 `run({ onEvent })` 观察运行时事件：

```ts
await task.run({
  onEvent(event) {
    console.log(event.type, event.timestamp, event.metadata)
  }
})
```

`onEvent` enables full internal event observation, including stage, channel, batch flush, and sink write events. For low-overhead progress reporting, prefer `progressBehavior()` or custom record/batch behavior hooks. See `docs/architecture/runtime.md` and `docs/architecture/performance-baseline.md` for the current cost model.

## Errors / 错误

`RuntimeError` includes `code`, `stage`, `input`, and `metadata`. Format-specific location details should live in `metadata`, not in core fields.

`RuntimeError` 包含 `code`、`stage`、`input` �?`metadata`。格式相关的位置详情应该放在 `metadata`，不要变�?core 字段�?
Core exports `CORE_RUNTIME_ERROR_CODES` as the stable error-code registry for framework-level failures. Adapters and codecs may still use package-specific string codes, but should prefer the shared names when the failure maps to a core category.

Core 导出 `CORE_RUNTIME_ERROR_CODES`，作为框架级失败的稳定错误码注册表。adapter �?codec 仍可使用包级 string code，但能映射到 core 分类时应优先复用共享名称�?
## Type Inference / 类型推导

The public pipeline builder carries the current item type across source, transform, plugin, decoder, encoder, batch, and sink boundaries:

公开 pipeline builder 会在 source、transform、plugin、decoder、encoder、batch �?sink 边界之间传递当�?item 类型�?
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

`Decoder<T>` stages are typed for `Uint8Array` streams, and `Encoder<T>` stages return `Uint8Array` streams. `npm run typecheck:api` protects these public inference contracts and the root/internal API boundary.

`Decoder<T>` 阶段�?`Uint8Array` 输入流建模，`Encoder<T>` 阶段输出 `Uint8Array` 流。`npm run typecheck:api` 会保护这些公开类型推导契约以及 root/internal API 边界�?
## Internal Boundary / 内部边界

The root `@robbin-io/core` entry is the stable public API surface. It intentionally exports user-facing builders, contracts, behaviors, transforms, errors, and metrics collectors.

`@robbin-io/core` 根入口是稳定公开 API 表面。它有意只导出面向用户的 builder、契约、behavior、transform、错误和 metrics collector�?
Execution helpers such as runtime state, runner functions, channel implementation, scheduler internals, and event factory helpers live under the explicit internal entry:

runtime state、runner 函数、channel 实现、scheduler 内部结构�?event factory helper 等执行辅助能力位于显式内部入口：

```ts
import { createRuntimeState } from "@robbin-io/core/internal"
```

`@robbin-io/core/internal` is available for framework-owned packages and advanced beta experiments, but it is not covered by the same compatibility promise as the root entry. Prefer the root entry for application code.

`@robbin-io/core/internal` 可供框架自有 package 和高�?beta 实验使用，但不享受与根入口相同的兼容性承诺。应用代码应优先使用根入口�?