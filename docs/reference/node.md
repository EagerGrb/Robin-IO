# Node API / Node API 参�?
`@robbin-io/node` provides Node-specific sources and sinks. It is the right place for file system, gzip, and Node stream adapters.

`@robbin-io/node` 提供 Node 专属 source �?sink。文件系统、gzip、Node stream 适配都属于这一层�?
## File Source / 文件 Source

```ts
import { fsFileSource } from "@robbin-io/node"

pipeline().from(fsFileSource("input.csv"))
```

## File Sink / 文件 Sink

```ts
import { fsFileSink } from "@robbin-io/node"

pipeline().to(fsFileSink("output.jsonl", { createParentDirectories: true }))
```

Use `atomic: true` to write to a temporary file and rename only after success.

使用 `atomic: true` 时，会先写临时文件，只在成功�?rename 到目标路径�?
```ts
fsFileSink("output.jsonl", { atomic: true, createParentDirectories: true })
```

## Gzip / Gzip 压缩

```ts
import { gzipFileSource, gzipFileSink } from "@robbin-io/node"

pipeline().from(gzipFileSource("input.jsonl.gz"))
pipeline().to(gzipFileSink("output.jsonl.gz", { atomic: true }))
```
