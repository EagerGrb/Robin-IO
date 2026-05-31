# TypeScript Import/Export Framework

A cross-runtime TypeScript streaming import/export framework for production-style data workflows.

This project is not just a CSV parser. It provides a full import/export runtime built around `AsyncIterable`, with adapters for Node and browser environments, codecs such as CSV and JSONL, reusable transforms, validation, progress reporting, structured errors, batching, cancellation, and package-level extension points.

## Why This Exists

Production imports and exports usually involve more than parsing:

- reading files or browser `Blob` objects;
- decoding records from CSV, JSONL, or another format;
- validating and transforming rows;
- tracking progress;
- collecting errors and dead letters;
- batching writes;
- exporting to files, browser downloads, streams, or memory;
- cancelling long-running work safely.

This framework models those steps as a single composable pipeline:

```text
Source -> Decoder? -> Transform* -> Encoder? -> Batch -> Sink
```

The core package is format-agnostic and platform-agnostic. Node, browser, CSV, JSONL, field mapping, and validation support live in separate packages.

## Quick Start

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
  .run({ runtime: "node" })
```

The example reads `users.csv`, decodes CSV rows, validates and transforms each row, encodes records as JSONL, then writes `users.jsonl`.

## Packages

- `@robbin-io/core`: pipeline builder, runtime executor, core contracts, metrics, progress/error behaviors, and `map`/`filter`/`validate` transforms.
- `@robbin-io/node`: Node `fs` source/sink, gzip file source/sink, atomic writes, and Node stream adapters.
- `@robbin-io/browser`: `Blob`/`File` source, download sink, `WritableStream` sink, and Web Worker transform adapter.
- `@robbin-io/source-file`: runtime-friendly file source entry point plus direct Node/browser exports.
- `@robbin-io/sink-file`: runtime-friendly file sink entry point plus direct Node/browser exports.
- `@robbin-io/codec-csv`: CSV decoder/encoder using `csv-parse` in Node and PapaParse in browser-like environments.
- `@robbin-io/codec-jsonl`: JSONL decoder/encoder.
- `@robbin-io/transform-fields`: field mapping helper for renaming, projection, defaults, required fields, and nested paths.
- `@robbin-io/validation-zod`: optional Zod validation transform.
- `@robbin-io/source-memory`: memory source.
- `@robbin-io/sink-memory`: memory sink and dead-letter sink.
- `@robbin-io/plugin-testing`: contract helpers for sources, sinks, and transforms.

## Commands

```bash
npm install
npm test
npm run verify
npm run typecheck
npm run typecheck:api
npm run build
npm run bench
npm run bench:stress
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
```

Use `npm run verify` for the full local quality gate. It covers formatting, tests, type checks, package build, package export checks, tarball checks, release-doc checks, Node ESM smoke, packed-consumer smoke, browser build, and browser smoke tests.

## Examples

- `examples/node-csv-to-jsonl`: Node CSV import, row transform, JSONL export.
- `examples/browser-csv-preview`: browser CSV upload preview with progress/error reporting.

Run the Node example:

```bash
npm run example:node
```

Run the browser example:

```bash
npm run example:browser
```

Build the browser example without keeping a dev server running:

```bash
npm run example:browser:build
```

## Performance Snapshot

The large import/export stress test uses a realistic file workflow:

```text
fsFileSource -> csvDecoder -> map -> jsonlEncoder -> fsFileSink
fsFileSource -> jsonlDecoder -> map -> csvEncoder -> fsFileSink
```

Recent 1,000,000-row stress results on Node 24:

| Scenario                  |         Throughput |      Peak RSS |
| ------------------------- | -----------------: | ------------: |
| CSV -> transform -> JSONL | about 66k rows/sec | about 277 MiB |
| JSONL -> transform -> CSV | about 82k rows/sec | about 261 MiB |

Run the stress test locally:

```bash
npm run bench:stress
```

See `benchmarks/large-import-export-stress-report-2026-05-18.md` and `docs/performance-large-import-export-plan.md` for details and caveats.

## API Reference

- [Getting Started](docs/getting-started.md)
- [Core](docs/reference/core.md)
- [Node](docs/reference/node.md)
- [Browser](docs/reference/browser.md)
- [Codecs](docs/reference/codecs.md)
- [Transforms and Validation](docs/reference/transforms.md)
- [Runtime Architecture](docs/architecture/runtime.md)
- [Release Checklist](docs/release/checklist.md)

## Release Status

The project is pre-1.0. Root package exports are the intended public API surface. Subpaths named `internal` are for framework-owned packages and advanced experiments, and they are not covered by the same compatibility promise.

See `docs/release/deployment-and-growth-plan.md` for the deployment, npm publishing, and growth plan.
