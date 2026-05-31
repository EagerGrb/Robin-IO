# Large Import/Export Performance Plan

Date: 2026-05-18

## Goal

Move the realistic Node file import/export path from about 18k-20k rows/sec toward 80k-150k rows/sec.

The reference workload is:

```text
fsFileSource -> csvDecoder -> map -> jsonlEncoder -> batch -> fsFileSink
fsFileSource -> jsonlDecoder -> map -> csvEncoder -> batch -> fsFileSink
```

The current 1,000,000-row stress result is:

| Scenario                  | Current throughput | Peak RSS |
| ------------------------- | -----------------: | -------: |
| CSV -> transform -> JSONL |    18,650 rows/sec |  234 MiB |
| JSONL -> transform -> CSV |    19,731 rows/sec |  242 MiB |

## Design Constraints

- Preserve the public pipeline API.
- Keep correctness, cancellation, close semantics, and atomic file sink behavior.
- Keep default observability useful, but allow optimized unobserved/direct paths to do less per-record work.
- Prefer general fast paths before adding one-off benchmark-only paths.
- Keep the first optimization pass small enough to verify with existing tests and the stress script.

## Current Hot Path Assessment

The current direct pipeline avoids the channel-backed runner when detailed events are not observed, but the workload is still expensive because:

- Encoders emit one `Uint8Array` chunk per logical row.
- `fsFileSink.write()` loops every item and awaits `writeChunk()` per item.
- The JSONL encoder calls `JSON.stringify()` per row and allocates a new encoded chunk per row.
- The CSV encoder builds and encodes a line per row.
- Progress behavior requires record and batch accounting across stages.
- CSV parsing is delegated to `csv-parse`, which is correct but not specialized for this benchmark shape.

The first bottleneck to remove is per-row file writes. Batching already groups rows, but the Node sink currently still writes each encoded row separately inside the batch.

## Optimization Roadmap

### P1: Batch-Aware Node File Sink

Make `fsFileSink` and `gzipFileSink` coalesce a batch of `Uint8Array|string` items into a small number of stream writes.

Expected impact:

- Reduces write calls from roughly 1,000,000 to roughly 200 for the default stress run.
- Keeps `Batch` and `Sink` API unchanged.
- Keeps bytes-written events by emitting per-batch byte totals rather than per-item byte events when no detailed observer is attached.

Implementation notes:

- Add `writeBatchChunks()` helper.
- For string batches, join into one string.
- For `Uint8Array` batches, use `Buffer.concat()` for multi-item batches.
- Preserve abort handling around the single stream write.
- Preserve detailed event behavior by still supporting byte metadata emission.

### P2: Encoder Chunk Coalescing

Add optional chunk aggregation inside JSONL and CSV encoders.

Possible API:

```ts
jsonlEncoder({ chunkSize: 64 * 1024 })
csvEncoder({ header, chunkSize: 64 * 1024 })
```

Implemented API:

```ts
jsonlEncoder({ chunkBytes: 64 * 1024 })
csvEncoder({ header, chunkBytes: 64 * 1024 })
```

Expected impact:

- Reduces async iterator yields between encoder and batch.
- Reduces batch array item count and memory churn.
- Keeps exact output bytes.

Risk:

- Progress currently treats encoder output chunks as handled records. Aggregating chunks changes metrics semantics unless stage-specific logical counts are introduced.

### P3: Fast Progress Counters

Allow `progressBehavior` to use summary counters from direct paths instead of per-record behavior callbacks.

Possible design:

- Add internal `notifyRecordSummary()` for direct path loops.
- Keep per-record events when `onEvent` is present.
- Preserve `ProgressSnapshot` logical meaning.

### P4: Codec-Specific Direct Pipelines

For common production paths, add explicit high-level helpers or internal fusion:

```text
CSV rows -> mapped objects -> JSONL lines
JSONL objects -> mapped objects -> CSV lines
```

This can reduce object churn and stage transitions, but it should come after P1-P3 because it is less general.

### P5: Native/WASM/Worker Acceleration

For 150k+ rows/sec sustained throughput on large files, evaluate:

- Worker-thread CSV decode.
- WASM/native CSV parser.
- Dedicated JSONL stringifier/CSV encoder optimized for stable schemas.
- DuckDB/Polars style columnar fast paths as optional connectors.

## Success Gates

| Gate       |                                         Target |
| ---------- | ---------------------------------------------: |
| P1 minimum | 35k+ rows/sec on at least one stress direction |
| P2 target  |                                  50k+ rows/sec |
| P3 target  |                                  80k+ rows/sec |
| P4 stretch |                             100k-150k rows/sec |

All gates must also keep:

- 1,000,000 output rows correct.
- `result.ok === true`.
- `errors.length === 0`.
- Peak RSS below 512 MiB for the reference workload.

## First Implementation Pass

Implement P1 first:

1. Coalesce file sink batch writes.
2. Keep source read behavior unchanged.
3. Run targeted Node and CSV tests.
4. Re-run the 1,000,000-row stress test.
5. Record before/after numbers in the stress report.
