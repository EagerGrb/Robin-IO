# Codec API / Codec API 参�?
Codec packages are optional plugins. The core does not depend on any file format.

Codec 包是可选插件。core 不依赖任何具体文件格式�?
## CSV

```ts
import { csvDecoder, csvEncoder } from "@robbin-io/codec-csv"

pipeline().through(csvDecoder({ header: true }))
pipeline().through(csvEncoder())
```

Node CSV decoding uses streaming `csv-parse`. Browser CSV decoding uses PapaParse's incremental chunk parser, so input chunks are decoded and parsed progressively instead of collected into one full string first.

Node CSV 解码使用流式 `csv-parse`。浏览器 CSV 解码使用 PapaParse 的增�?chunk parser，因此输�?chunk 会逐步 decode/parse，而不是先收集成完整字符串�?
CSV decode failures are reported as `RuntimeError` with `code: "CODEC_DECODE_ERROR"` and parser metadata such as `format`, `parser`, `parserCode`, `line`, `row`, `column`, or `index` when the underlying parser provides it.

CSV 解码失败会以 `RuntimeError` 返回，`code` �?`"CODEC_DECODE_ERROR"`，并尽量携带底层解析器提供的 `format`、`parser`、`parserCode`、`line`、`row`、`column` �?`index` metadata�?
`csvEncoder({ escapeFormula: true })` prefixes spreadsheet formula-looking values with `'` to reduce CSV formula injection risk. The default remains `false` for compatibility with existing exports.

`csvEncoder({ escapeFormula: true })` 会给类似电子表格公式的值加 `'` 前缀，以降低 CSV formula injection 风险。默认值仍�?`false`，用于保持既有导出兼容性�?
## JSONL

```ts
import { jsonlDecoder, jsonlEncoder } from "@robbin-io/codec-jsonl"

pipeline().through(jsonlDecoder())
pipeline().through(jsonlEncoder())
```
