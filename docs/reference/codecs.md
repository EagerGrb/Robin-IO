# Codec API

Codec packages are optional plugins. The core does not depend on any file format.

## CSV

```ts
import { csvDecoder, csvEncoder } from "@robbin-io/codec-csv"

pipeline().through(csvDecoder({ header: true }))
pipeline().through(csvEncoder())
```

Node CSV decoding uses streaming `csv-parse`. Browser CSV decoding uses PapaParse's incremental chunk parser, so input chunks are decoded and parsed progressively instead of collected into one full string first.

CSV decode failures are reported as `RuntimeError` values with `code: "CODEC_DECODE_ERROR"` and parser metadata such as `format`, `parser`, `parserCode`, `line`, `row`, `column`, or `index` when the underlying parser provides it.

`csvEncoder({ escapeFormula: true })` prefixes spreadsheet formula-looking values with `'` to reduce CSV formula injection risk. The default remains `false` for compatibility with existing exports.

`csvEncoder({ chunkBytes })` can coalesce encoded rows into larger byte chunks for better large-file throughput.

## JSONL

```ts
import { jsonlDecoder, jsonlEncoder } from "@robbin-io/codec-jsonl"

pipeline().through(jsonlDecoder())
pipeline().through(jsonlEncoder())
```

`jsonlDecoder({ maxLineBytes })` protects against oversized lines.

`jsonlEncoder({ chunkBytes })` can coalesce encoded JSONL lines into larger byte chunks for better large-file throughput.
