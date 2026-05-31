# Large Import/Export Stress Test Report

Date: 2026-05-18

## Scope

This run targets a realistic large file import/export path:

1. Generate a 1,000,000-row CSV file.
2. Import CSV through `fsFileSource -> csvDecoder -> map -> jsonlEncoder -> fsFileSink`.
3. Export JSONL through `fsFileSource -> jsonlDecoder -> map -> csvEncoder -> fsFileSink`.

The test uses real filesystem streaming sources/sinks, codecs, transforms, batching, progress behavior, and atomic file output.

## Environment

| Item                   | Value                                                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Project                | `C:\robin\io`                                                                                                         |
| Node                   | `v24.13.0`                                                                                                            |
| Platform               | `win32 x64`                                                                                                           |
| Command                | `STRESS_ROWS=1000000 STRESS_BATCH_SIZE=5000 STRESS_CHUNK_SIZE=1048576 node benchmarks/large-import-export-stress.mjs` |
| Batch size             | 5,000                                                                                                                 |
| File source chunk size | 1 MiB                                                                                                                 |

## Dataset

| File          |                            Rows |       Size |
| ------------- | ------------------------------: | ---------: |
| Input CSV     | 1,000,000 data rows plus header |  64.39 MiB |
| Output JSONL  |                 1,000,000 lines | 128.31 MiB |
| Roundtrip CSV | 1,000,000 data rows plus header |  67.27 MiB |

## Results

| Scenario                  | Result       | Duration |  Rows/sec | Input MiB/sec | Output MiB/sec |   Peak RSS | Peak heap |
| ------------------------- | ------------ | -------: | --------: | ------------: | -------------: | ---------: | --------: |
| CSV -> transform -> JSONL | OK, 0 errors |  53.62 s | 18,650.52 |          1.20 |           2.39 | 234.35 MiB | 84.12 MiB |
| JSONL -> transform -> CSV | OK, 0 errors |  50.68 s | 19,731.21 |          2.53 |           1.33 | 242.24 MiB | 86.74 MiB |

## Progress And Metrics

| Scenario     | recordsRead | recordsHandled | recordsWritten | batchesWritten | Output rows |
| ------------ | ----------: | -------------: | -------------: | -------------: | ----------: |
| CSV -> JSONL |          65 |      3,000,000 |      1,000,000 |            200 |   1,000,000 |
| JSONL -> CSV |         129 |      3,000,001 |      1,000,001 |            201 |   1,000,000 |

Notes:

- `recordsRead` is source chunk count here, not logical row count.
- `recordsHandled` includes multiple pipeline stages, so it is expected to exceed the logical row count.
- JSONL -> CSV writes one extra record because `csvEncoder` emits a header row.

## Finding

The initial 200,000-row validation run exposed a `MaxListenersExceededWarning` in the CSV streaming decoder. Root cause: `waitForDrain()` added a one-time `error` listener that was not removed after `drain`. This has been fixed in `packages/codec-csv/src/index.ts`, and the 1,000,000-row run completed without the warning.

## Conclusion

The current Node large import/export path is functionally stable for a 1,000,000-row filesystem workload. Throughput is about 18.6k-19.7k rows/sec, while RSS stays bounded around 240 MiB. The biggest production risk observed during this test was listener cleanup under long streaming CSV input, which is now fixed.

Recommended next checks:

- Run with 5,000,000 rows to measure longer-duration memory stability.
- Add a dirty CSV case with validation failures and dead-letter output.
- Compare `batchSize` values such as 1,000, 5,000, and 20,000 for write latency and memory tradeoffs.
