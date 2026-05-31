# Node API

`@robbin-io/node` provides Node-specific sources and sinks. It is the package for filesystem, gzip, and Node stream adapters.

## File Source

```ts
import { fsFileSource } from "@robbin-io/node"

pipeline().from(fsFileSource("input.csv"))
```

`fsFileSource(path, { chunkSize })` reads a file as `Uint8Array` chunks and emits byte metadata on record events.

## File Sink

```ts
import { fsFileSink } from "@robbin-io/node"

pipeline().to(fsFileSink("output.jsonl", { createParentDirectories: true }))
```

Use `atomic: true` to write to a temporary file and rename only after a successful pipeline:

```ts
fsFileSink("output.jsonl", { atomic: true, createParentDirectories: true })
```

The file sink coalesces each batch into a single write where possible. This keeps the public sink contract unchanged while reducing write overhead for large encoded exports.

## Gzip

```ts
import { gzipFileSource, gzipFileSink } from "@robbin-io/node"

pipeline().from(gzipFileSource("input.jsonl.gz"))
pipeline().to(gzipFileSink("output.jsonl.gz", { atomic: true }))
```

## Stream Adapters

```ts
import { readableSource, writableSink } from "@robbin-io/node"

pipeline().from(readableSource(readable))
pipeline().to(writableSink(writable))
```

The stream adapters participate in the same cancellation, byte metadata, and close semantics as file sources and sinks.
