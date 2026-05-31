# TypeScript Import/Export Framework 使用指南

这是一套跨运行时的 TypeScript 流式导入导出框架。它的目标不是只做一�?CSV 工具，而是提供一个稳定的导入导出内核：你可以把文件、浏览器 Blob、内存数据、Node stream、CSV、JSONL、字段映射、校验、Worker transform、进度统计、错误收集等能力组合成一条可观察、可取消、可测试�?pipeline�?
当前成熟度记录：97/100。核�?runtime、Node/Browser 主路径、CSV/JSONL codec、真实浏览器 smoke、Playwright 多浏览器矩阵、API 类型门禁、package export 检查、npm tarball dry-run 门禁、clean install packed tarball consumer smoke �?CI/npm 发布 workflow 已经成型。正式发布前仍需要在真实仓库配置 `NPM_TOKEN`、环境审批和 main 分支发布流程�?
## 1. 这个框架做什�?
导入导出通常包含读取、解析、转�?��校验、错误处理、进度统计和写入。如果这些逻辑都写在一个函数里，代码很快会变得难测、难取消、难观测，也很难同时支持 Node 和浏览器�?
本框架把导入导出拆成统一的流式模型：

```text
Source -> Decoder? -> Transform* -> Encoder? -> Batch -> Sink
```

- `Source<T>` 产生输入，例如文件、Blob、内存数据�?- `Decoder<T>` �?`Uint8Array` 解码成记录，例如 CSV/JSONL�?- `Transform<I, O>` 做过滤、映射、校验、字段清洗�?- `Encoder<T>` 把记录编码回 `Uint8Array`�?- `Batch<T>` 控制批量写入�?- `Sink<T>` 输出到文件、内存、浏览器下载�?`WritableStream`�?
Core 不绑�?Node、浏览器、CSV、JSONL 或业务规则。平台能力在 adapter 包里，格式能力在 codec 包里，业务能力在 transform/validation/plugin 层里�?
## 2. 安装与本地运�?
在本仓库中开发：

```bash
npm install
npm run verify
```

发布后，应用侧按需要安装对应包�?
```bash
npm install @robbin-io/core @robbin-io/codec-csv @robbin-io/codec-jsonl @robbin-io/node
```

浏览器项目通常安装�?
```bash
npm install @robbin-io/core @robbin-io/browser @robbin-io/codec-csv @robbin-io/sink-memory
```

## 3. 最�?pipeline

```ts
import { pipeline, map } from "@robbin-io/core"

const result = await pipeline()
  .from(source)
  .through(map((record) => record))
  .batch({ size: 1000 })
  .to(sink)
  .run()

if (!result.ok) {
  console.error(result.errors)
}
```

`run()` 返回�?
```ts
interface RunResult {
  readonly ok: boolean
  readonly metrics: Record<string, number>
  readonly errors: readonly RuntimeError[]
}
```

Pipeline builder 会携带当�?item 类型�?
```ts
import { map, pipeline, type Sink, type Source } from "@robbin-io/core"

type CsvRow = { id: string; name: string }
type User = { id: number; name: string }

declare const source: Source<CsvRow>
declare const sink: Sink<User>

await pipeline()
  .from(source)
  .through(map((row) => ({ id: Number(row.id), name: row.name.trim() })))
  .to(sink)
  .run()
```

`Decoder<T>` 只能接在 `Uint8Array` 流后面，`Encoder<T>` 会输�?`Uint8Array`。这些公�?API 类型契约�?`npm run typecheck:api` 保护�?
## 4. Node：CSV 导入，JSONL 导出

```ts
import { pipeline, map, validate } from "@robbin-io/core"
import { csvDecoder } from "@robbin-io/codec-csv"
import { jsonlEncoder } from "@robbin-io/codec-jsonl"
import { fsFileSource, fsFileSink } from "@robbin-io/node"

await pipeline()
  .from(fsFileSource("users.csv"))
  .through(csvDecoder({ header: true }))
  .through(validate((row: any) => Boolean(row.id), "row.id is required"))
  .through(
    map((row: any) => ({
      id: Number(row.id),
      name: String(row.name ?? "").trim()
    }))
  )
  .through(jsonlEncoder())
  .batch({ size: 1000 })
  .to(fsFileSink("users.jsonl", { atomic: true, createParentDirectories: true }))
  .run({ runtime: "node" })
```

要点�?
- `fsFileSource()` 读取 Node 文件�?- `csvDecoder({ header: true })` �?CSV 行解析成对象�?- `validate()` 做轻量校验�?- `map()` 做业务转�?�?- `jsonlEncoder()` 输出 JSONL bytes�?- `fsFileSink(..., { atomic: true })` 先写临时文件，成功后 rename，避免失败留下半成品�?
## 5. 浏览器：CSV 上传预览

```ts
import { blobSource, webWorkerTransform } from "@robbin-io/browser"
import { csvDecoder } from "@robbin-io/codec-csv"
import { pipeline, progressBehavior } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"

const file = input.files![0]
const sink = memorySink<Record<string, string>>()
const progress = progressBehavior()

await pipeline()
  .from(blobSource(file, { chunkSize: 64 * 1024 }))
  .through(csvDecoder({ header: true }))
  .through(
    webWorkerTransform<Record<string, string>, Record<string, string>>(
      () => new Worker(new URL("./normalize.worker.ts", import.meta.url), { type: "module" }),
      { name: "normalize-rows", concurrency: 8, maxPending: 16 }
    )
  )
  .batch({ size: 500 })
  .to(sink)
  .run({
    runtime: "browser",
    behaviors: [progress],
    errorMode: "skip-and-collect"
  })

console.log(progress.getSnapshot())
console.log(sink.getItems().slice(0, 20))
```

浏览�?CSV decoder 使用 PapaParse 的增�?chunk parser，不会先把整个文�?collect 成完整字符串。真�?Chrome smoke 已覆�?Browser CSV、CSV+Worker�?0k �?CSV large smoke、Worker transfer �?WritableStream export�?
## 6. 浏览器：导出文件

中小文件下载�?
```ts
import { downloadSink } from "@robbin-io/browser"
import { csvEncoder } from "@robbin-io/codec-csv"
import { pipeline } from "@robbin-io/core"

const sink = downloadSink({ type: "text/csv", name: "download-users" })

await pipeline()
  .from(source)
  .through(csvEncoder({ header: ["id", "name"], escapeFormula: true }))
  .to(sink)
  .run({ runtime: "browser" })

const url = sink.getObjectUrl()
downloadAnchor.href = url
downloadAnchor.download = "users.csv"
sink.dispose()
```

大文件或原生流式导出�?
```ts
import { writableStreamSink } from "@robbin-io/browser"
import { csvEncoder } from "@robbin-io/codec-csv"
import { pipeline } from "@robbin-io/core"

const stream = await fileHandle.createWritable()

await pipeline()
  .from(source)
  .through(csvEncoder({ header: ["id", "name"], escapeFormula: true }))
  .to(writableStreamSink(stream, { name: "users-export" }))
  .run({ runtime: "browser" })
```

`writableStreamSink()` 会直接写�?stream，成功时 close writer，pipeline 失败或取消时 abort writer，并提供 `getBytesWritten()` �?`getChunksWritten()`�?
## 7. 字段映射与校�?
字段映射�?
```ts
import { mapFields } from "@robbin-io/transform-fields"

const normalize = mapFields<{ "User ID": string; profile?: { name?: string } }, { id: string; name: string }>({
  id: { from: "User ID", required: true },
  name: {
    from: "profile.name",
    default: "Unknown",
    transform: (value) => String(value).trim()
  }
})
```

Zod 校验�?
```ts
import { z } from "zod"
import { validateWithZod } from "@robbin-io/validation-zod"

const userSchema = z.object({
  id: z.coerce.number(),
  name: z.string().min(1)
})

pipeline().through(validateWithZod(userSchema))
```

校验失败会变�?`RuntimeError`，带有结构化 metadata，适合写入 dead-letter 或错误报告�?
## 8. 进度、错误和取消

低成本进度：

```ts
import { pipeline, progressBehavior } from "@robbin-io/core"

const progress = progressBehavior()

await pipeline()
  .from(source)
  .to(sink)
  .run({
    behaviors: [progress]
  })

console.log(progress.getSnapshot())
```

完整事件观察�?
```ts
await pipeline()
  .from(source)
  .to(sink)
  .run({
    onEvent(event) {
      console.log(event.type, event.timestamp)
    }
  })
```

`onEvent` 会保留完�?stage、channel、batch flush、sink write 等事件，适合诊断和审计，但成本明显高于默认路径�?
跳过坏记录并收集错误�?
```ts
const result = await pipeline().from(source).through(transform).to(sink).run({
  errorMode: "skip-and-collect"
})

for (const error of result.errors) {
  console.error(error.code, error.stage, error.metadata)
}
```

取消�?
```ts
const controller = new AbortController()
const task = pipeline().from(source).to(sink)
const running = task.run({ signal: controller.signal })

controller.abort("user cancelled")

const result = await running
console.log(result.ok) // false
```

资源关闭顺序、并�?transform fail-fast、随机取消点都已有测试覆盖�?
## 9. 包和接口索引

`@robbin-io/core`�?
- `pipeline()`�?- `PipelineBuilder` / `PipelineTask`�?- `Source<T>` / `Sink<T>` / `Transform<I, O>`�?- `Decoder<T>` / `Encoder<T>`�?- `RuntimeBehavior`�?- `RuntimeError` / `CORE_RUNTIME_ERROR_CODES`�?- `map()` / `filter()` / `validate()` / `transform()`�?- `progressBehavior()` / `errorReporterBehavior()` / `deadLetterBehavior()` / `cancellationBehavior()`�?- `InMemoryMetricsCollector`�?
`@robbin-io/node`�?
- `fsFileSource()`�?- `fsFileSink()`�?- `gzipFileSource()`�?- `gzipFileSink()`�?- Node readable/writable stream adapters�?
`@robbin-io/browser`�?
- `blobSource()` / `fileSource()`�?- `downloadSink()`�?- `writableStreamSink()`�?- `webWorkerTransform()`�?- `exposeWorkerTransform()`�?
`@robbin-io/codec-csv`�?
- `csvDecoder({ header, delimiter, skipEmptyLines, browserChunkSize })`�?- `csvEncoder({ header, delimiter, escapeFormula })`�?
`@robbin-io/codec-jsonl`�?
- `jsonlDecoder({ ignoreEmptyLines, maxLineBytes })`�?- `jsonlEncoder()`�?
其他包：

- `@robbin-io/source-memory`: `memorySource()`�?- `@robbin-io/sink-memory`: `memorySink()` / `deadLetterSink()`�?- `@robbin-io/transform-fields`: `mapFields()`�?- `@robbin-io/validation-zod`: `validateWithZod()`�?- `@robbin-io/plugin-testing`: 自定�?source/sink/transform 的契约测试工具�?
## 10. 性能基线

当前性能数据来自本地 benchmark，不是正�?SLA，但可以作为容量预估和回归判断依据�?
| 场景                                         | 当前稳定均�?|
| -------------------------------------------- | -----------: |
| 10k records 简�?map                         |      46-50ms |
| 10k records progress behavior                |      58-60ms |
| 10k records full observer                    |    225-235ms |
| CSV decode 10k rows                          |      51-52ms |
| CSV decode 100k rows                         |    522-537ms |
| 100k memory pipeline map+batch+sink          |    520-605ms |
| batch write 10k records                      |      25-27ms |
| slow sink 10k records, 1ms/batch             |      38-41ms |
| Browser Worker ArrayBuffer clone 10x64KiB    |    24-29.5ms |
| Browser Worker ArrayBuffer transfer 10x64KiB |     8-10.5ms |

性能理解�?
- 默认路径是高性能路径�?- `progressBehavior()` 适合生产 UI 进度，开销较低�?- `onEvent` 是完整观察路径，适合诊断，不适合大文件常态热路径�?- Worker `transfer(input)` 对可转移�?`ArrayBuffer` 明显快于 structured clone�?- 并发 transform 对真正异步或重计算任务有意义，对立即 resolve 的同�?map 不一定更快�?
回归护栏：默�?map、progress、CSV decode�?00k memory pipeline 如果持续退化超�?10%，发布前需要解释或修复�?
## 11. 当前质量�?
完整本地质量门：

```bash
npm run verify
```

目前覆盖�?
- Prettier 格式检查；
- 16 个测试文件，118 个测试；
- TypeScript project typecheck�?- API 类型推导�?root/internal 边界回归�?- build�?- package export 实际 import 检查；
- public API surface 检查；
- release docs 检查；
- npm tarball dry-run 内容检查；
- clean install packed tarball consumer smoke�?- Changesets config 检查；
- Node ESM dist smoke�?- 浏览器示�?build�?- 真实 Chrome Browser CSV smoke�?- 真实 Chrome Browser CSV + Worker smoke�?- 真实 Chrome Browser WritableStream smoke�?- 真实 Chrome Browser Worker smoke�?- Playwright Chromium/Firefox/WebKit browser matrix smoke�?- release workflow dry-run publish�?
## 12. 已完成开发总结

核心 runtime�?
- 建立 `pipeline().from().through().batch().to().run()` �?API�?- 完成 source/decoder/transform/encoder/batch/sink 线�?runtime�?- 引入有界 channel、runner、scheduler、stable stage id�?- 补齐 direct/fused fast path�?- 统一 metrics、events、behavior hooks�?- 完成取消、fail-fast、skip-and-collect、preserveOrder、backpressure 基础语义�?- 增加 source/transform/sink/abort 故障注入�?- 增加并发 transform fail-fast �?ordered completion 释放测试�?- 增加随机取消�?property 测试�?
API 与类型：

- `@robbin-io/core` root entry 收敛为稳定公开 API�?- `@robbin-io/core/internal` 承载内部 runtime/channel/runner/scheduler 能力�?- 新增 API type regression gate�?- 收紧 decoder 类型边界，避免错误接到非 bytes stream�?
Node 与浏览器�?
- Node fs source/sink、gzip、atomic sink、stream adapter 基础语义成型�?- Browser Blob/File source 支持 chunkSize �?bytes metadata�?- `downloadSink()` 支持 object URL 生命周期�?dispose�?- `writableStreamSink()` 支持流式导出、close/abort�?- `webWorkerTransform()` 支持 concurrency、maxPending、timeout、cancel、transfer�?- 真实 Chrome smoke 覆盖 CSV、CSV+Worker、WritableStream、Worker�?
Codec �?transform�?
- CSV Node 路径使用 `csv-parse` streaming�?- CSV Browser 路径使用 PapaParse 增量 chunk parser�?- CSV decode metadata �?formula guard�?- JSONL line/raw/maxLineBytes�?- CSV/JSONL round-trip �?chunk boundary property 测试�?- 字段映射�?Zod 校验插件�?
发布质量�?
- package export import gate�?- public API surface allowlist�?- release docs check�?- Changesets config check�?- npm tarball dry-run gate�?- Node dist smoke�?- browser smoke 矩阵�?- 成熟度评估文档和 release readiness 文档�?
## 13. 还未达到 100% 的原�?
这套框架已经接近真实发布水平，但还不能诚实说 100%�?
- 还缺低内存设备或大文件长期内存曲线；
- File System Access API 真实 handle 场景还需要单�?smoke�?- 连接�?格式生态还少，例如 Excel、Parquet、数据库、S3/object storage�?- 正式 npm 发布仍需要在真实仓库配置 `NPM_TOKEN`、环境审批、main 分支保护�?release workflow 人工批准�?
下一步最推荐补低内存/大文件长期曲线和 File System Access API 真实 handle 场景�?