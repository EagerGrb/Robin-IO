# TypeScript Import/Export Framework / TypeScript 导入导出框架

This repository is the MVP monorepo described in `typescript-import-export-implementation-guide.md`.

本仓库是 `typescript-import-export-implementation-guide.md` 中描述的 MVP monorepo 落地版本�?
It provides a cross-runtime streaming import/export kernel built around `AsyncIterable`, with package-level adapters for Node, browser, memory sources/sinks, plugin testing, and optional format codecs such as CSV and JSONL.

它提供了一个基�?`AsyncIterable` 的跨运行时流式导入导出内核，并配�?Node、浏览器、内�?source/sink、插件测试，以及 CSV、JSONL 等可选格�?codec�?
The core is format-agnostic and business-agnostic. CSV and JSONL are official codec plugins, not framework assumptions.

核心保持格式无关和业务无关。CSV �?JSONL 是官�?codec 插件，不是框架内核的前提假设�?
## Quick Start / 快速开�?
```ts
import { pipeline, map, validate } from "@robbin-io/core"
import { csvDecoder } from "@robbin-io/codec-csv"
import { jsonlEncoder } from "@robbin-io/codec-jsonl"
import { fsFileSource, fsFileSink } from "@robbin-io/node"

await pipeline()
  .from(fsFileSource("users.csv"))
  .through(csvDecoder({ header: true }))
  .through(validate((row: any) => Boolean(row.id), "row.id is required"))
  .through(map((row: any) => ({ id: row.id, name: String(row.name ?? "").trim() })))
  .through(jsonlEncoder())
  .batch({ size: 1000 })
  .to(fsFileSink("users.jsonl"))
  .run()
```

The example reads `users.csv`, decodes CSV rows, validates and transforms each row, encodes records as JSONL, then writes `users.jsonl`.

上面的示例会读取 `users.csv`，解�?CSV 行，校验并转换每条记录，再编码成 JSONL，最后写�?`users.jsonl`�?
## Packages / 包说�?
- `@robbin-io/core`: pipeline builder, runtime executor, core types, metrics, progress/error behavior, map/filter/validate transforms.
  `pipeline` 构建器、运行时执行器、核心类型、指标、进�?错误行为，以�?`map/filter/validate` transform�?- `@robbin-io/node`: Node `fs` source/sink and stream adapters.
  Node `fs` 文件 source/sink、gzip file source/sink �?stream 适配器�?- `@robbin-io/browser`: `Blob`/`File` source, download sink, WritableStream sink, and Web Worker transform.
  浏览�?`Blob`/`File` source、下�?sink、WritableStream sink �?Web Worker transform�?- `@robbin-io/source-file`: runtime-friendly file source entry point plus direct Node/browser exports.
  运行时友好的文件 source 入口，同时导�?Node/浏览器的直接实现�?- `@robbin-io/sink-file`: runtime-friendly file sink entry point plus direct Node/browser exports.
  运行时友好的文件 sink 入口，同时导�?Node/浏览器的直接实现�?- `@robbin-io/codec-csv`: CSV decoder/encoder, wrapping `csv-parse` in Node and `papaparse` in browser-like environments.
  CSV decoder/encoder；Node 环境封装 `csv-parse`，浏览器类环境封�?`papaparse`�?- `@robbin-io/codec-jsonl`: JSONL decoder/encoder.
  JSONL decoder/encoder�?- `@robbin-io/transform-fields`: field mapping helper for renaming, projection, defaults, and nested paths.
  字段映射 helper，用于重命名、投影、默认值和嵌套路径处理�?- `@robbin-io/validation-zod`: optional Zod validation transform.
  可选的 Zod 校验 transform�?- `@robbin-io/source-memory`: memory source.
  内存 source�?- `@robbin-io/sink-memory`: memory sink and dead-letter sink.
  内存 sink �?dead-letter sink�?- `@robbin-io/plugin-testing`: contract helpers for sources, sinks, and transforms.
  source、sink、transform 的契约测试辅助工具�?
## Commands / 命令

```bash
npm install
npm test
npm run verify
npm run typecheck
npm run typecheck:api
npm run bench
npm run package:check
npm run package:tarballs:check
npm run changeset:check-config
npm run changeset:status
npm run example:node
npm run example:browser:build
npm run example:browser
npm run smoke:node-dist
npm run smoke:packed-consumer
npm run smoke:browser-matrix
npm run smoke:browser-writable-stream
npm run release:publish:dry-run
npm run build
```

Use `npm test` for unit and end-to-end tests, `npm run verify` for the full local quality gate, `npm run bench` for local performance baselines, `npm run package:check` for package export validation, `npm run package:tarballs:check` for npm tarball dry-run content validation, `npm run changeset:check-config` for local Changesets config checks, `npm run changeset:status` for release-note status in a git branch, `npm run example:node` for the Node CSV-to-JSONL example, `npm run example:browser` for the Vite browser preview, `npm run smoke:node-dist` to verify built ESM package entries, `npm run smoke:packed-consumer` to verify clean install from packed tarballs, `npm run smoke:browser-matrix` for Playwright Chromium/Firefox/WebKit smoke, `npm run smoke:browser-csv` for real browser CSV streaming smoke, `npm run smoke:browser-csv-large` for a larger real browser CSV boundary smoke, `npm run smoke:browser-csv-worker` for real browser CSV plus Worker transform smoke, `npm run smoke:browser-writable-stream` for real browser WritableStream export smoke, `npm run smoke:browser-worker` for real browser Worker smoke, `npm run release:publish:dry-run` for publish simulation, `npm run typecheck` for TypeScript project checks, `npm run typecheck:api` for public API type inference and root/internal boundary regression checks, and `npm run build` for package output.

使用 `npm test` 运行单元测试和端到端测试，使�?`npm run verify` 运行完整本地质量门，使用 `npm run bench` 运行本地性能基准，使�?`npm run package:check` 校验 package 导出，使�?`npm run package:tarballs:check` 校验 npm tarball dry-run 文件清单，使�?`npm run changeset:check-config` 做本�?Changesets 配置检查，使用 `npm run changeset:status` �?git 分支中检查发布说明状态，使用 `npm run example:node` 运行 Node CSV �?JSONL 示例，使�?`npm run example:browser` 运行 Vite 浏览器预览示例，使用 `npm run smoke:node-dist` 验证构建后的 ESM package 入口，使�?`npm run smoke:packed-consumer` 验证 clean install packed tarball 消费路径，使�?`npm run smoke:browser-csv` 运行真实浏览�?CSV streaming smoke，使�?`npm run smoke:browser-csv-large` 运行更大的真实浏览器 CSV 边界 smoke，使�?`npm run smoke:browser-csv-worker` 运行真实浏览�?CSV �?Worker transform 组合 smoke，使�?`npm run smoke:browser-writable-stream` 运行真实浏览�?WritableStream 导出 smoke，使�?`npm run smoke:browser-worker` 运行真实浏览�?Worker smoke，使�?`npm run typecheck` 运行 TypeScript 项目检查，使用 `npm run typecheck:api` 运行公开 API 类型推导�?root/internal 边界回归检查，使用 `npm run build` 生成�?package 的构建产物�?
## Workflows / 常用工作�?
For day-to-day development, run tests and type checks before building:

日常开发时，建议先运行测试和类型检查，再构建：

```bash
npm test
npm run typecheck
npm run build
```

To verify that the built package entries work in plain Node ESM, run:

如果要验证构建后�?package 入口能被原生 Node ESM 正常导入，运行：

```bash
npm run smoke:node-dist
```

To run the current Node example, edit `examples/node-csv-to-jsonl/input.csv`, then run:

如果要运行当�?Node 示例，先编辑 `examples/node-csv-to-jsonl/input.csv`，然后运行：

```bash
npm run example:node
```

The generated file is written to `examples/node-csv-to-jsonl/output.jsonl`.

生成结果会写�?`examples/node-csv-to-jsonl/output.jsonl`�?
To run the browser preview, start the Vite dev server:

如果要运行浏览器预览示例，启�?Vite dev server�?
```bash
npm run example:browser
```

To verify the browser example without keeping a server running:

如果只想验证浏览器示例能构建，不保持服务运行�?
```bash
npm run example:browser:build
```

## Examples / 示例

- `examples/node-csv-to-jsonl`: Node CSV import, row transform, JSONL export.
  Node CSV 导入、行转换、JSONL 导出�?- `examples/browser-csv-preview`: browser CSV upload preview with progress/error reporting.
  浏览�?CSV 上传预览，并显示进度/错误报告�?
## API Reference / API 参�?
- [Getting Started / 使用指南](docs/getting-started.md)
- [Core](docs/reference/core.md)
- [Node](docs/reference/node.md)
- [Browser](docs/reference/browser.md)
- [Codecs](docs/reference/codecs.md)
- [Transforms and Validation](docs/reference/transforms.md)
- [Release Checklist](docs/release/checklist.md)

## MVP Boundaries / MVP 边界

The first version intentionally keeps the runtime linear:

第一版会有意保持线性运行时�?
```text
source -> through* -> batch -> sink
```

The core owns API shape, execution, cancellation, metrics, and behavior hooks. Platform and format concerns stay in adapter packages.

核心包负�?API 形态、执行、取消、指标和行为钩子。平台相关能力和格式相关能力留在各自的适配 package 中�?