# Benchmark Baselines

This directory records repeatable performance baselines for the streaming IO core.

## 2026-05-11 MVP Baseline

- Machine: Intel Xeon Processor (Skylake, IBRS), 8 GB RAM
- OS: Microsoft Windows 10.0.20348, x64
- Node: v24.13.0
- npm: 11.6.2
- Command: `npm run bench`

| Benchmark                  | ops/sec | mean ms | samples |
| -------------------------- | ------: | ------: | ------: |
| transform: map 10k records |      15 |  65.172 |      64 |
| csv: decode 10k rows       |      30 |  33.703 |      64 |
| batch: write 10k records   |      33 |  30.069 |      64 |

Quality gate captured at the same baseline: `npm run verify` passed.

## Running

The benchmark runner accepts optional sampling controls:

- `BENCH_TIME` defaults to `100`
- `BENCH_ITERATIONS` defaults to `8`
- `BENCH_WARMUP_TIME` defaults to `25`
- `BENCH_WARMUP_ITERATIONS` defaults to `2`

Use larger values when capturing release baselines and smaller values when checking a development branch.

Worker transfer benchmarks also accept payload controls:

- `BENCH_TRANSFER_PAYLOAD_SIZE` defaults to `262144` bytes.
- `BENCH_TRANSFER_PAYLOAD_COUNT` defaults to `200`.

## 2026-05-11 TransformRunner Quick Baseline

- Command: `BENCH_TIME=50 BENCH_ITERATIONS=3 BENCH_WARMUP_TIME=10 BENCH_WARMUP_ITERATIONS=1 npm run bench`
- Note: quick sampling is for directional comparison only.

| Benchmark                                               | ops/sec |  mean ms | samples |
| ------------------------------------------------------- | ------: | -------: | ------: |
| transform: map 10k records                              |       4 |  263.703 |       3 |
| transform-runner: async map 10k concurrency=1           |       4 |  266.828 |       3 |
| transform-runner: async map 10k concurrency=8 unordered |       4 |  268.764 |       3 |
| transform-runner: async map 10k concurrency=8 ordered   |       3 |  286.673 |       3 |
| csv: decode 10k rows                                    |      21 |   48.917 |       3 |
| batch: write 10k records                                |      32 |   31.018 |       3 |
| pipeline: map+batch+memory 100k records                 |       0 | 2716.041 |       3 |
| pipeline: slow sink 10k records 1ms/batch               |      21 |   48.492 |       3 |

## 2026-05-12 Phase 1 Production Semantics Baseline

- Command: `npm run bench`
- Note: this baseline was captured after internal event dispatch, stable stage ids, codec runners, JSONL error metadata, and Node stream hardening. It intentionally records the current performance cost before the dedicated performance phase.

| Benchmark                                               | ops/sec |  mean ms | samples |
| ------------------------------------------------------- | ------: | -------: | ------: |
| transform: map 10k records                              |       2 |  634.467 |       8 |
| transform-runner: async map 10k concurrency=1           |       2 |  624.289 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       2 |  578.849 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       2 |  578.700 |       8 |
| csv: decode 10k rows                                    |       2 |  428.952 |       8 |
| batch: write 10k records                                |       5 |  217.873 |       8 |
| pipeline: map+batch+memory 100k records                 |       0 | 6290.879 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |       4 |  225.174 |       8 |

Quality gate captured at the same baseline: `npm run verify` passed with 15 test files and 85 tests.

## 2026-05-12 Phase 2 Performance Pass 1

- Command: `npm run bench`
- Note: first performance pass after metrics max fast path, event observer cache, sync emit fast path, summary mode for unobserved stage/channel events, channel metric key caching, and direct record/batch metrics. Results still need more runs before release gating because some transform numbers fluctuate.

| Benchmark                                               | ops/sec |  mean ms | samples |
| ------------------------------------------------------- | ------: | -------: | ------: |
| transform: map 10k records                              |       3 |  347.882 |       8 |
| transform-runner: async map 10k concurrency=1           |       4 |  281.956 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       4 |  270.167 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       4 |  274.709 |       8 |
| csv: decode 10k rows                                    |       5 |  182.790 |       8 |
| batch: write 10k records                                |       8 |  118.675 |       8 |
| pipeline: map+batch+memory 100k records                 |       0 | 2800.266 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |       8 |  132.009 |       8 |

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 86 tests.

## 2026-05-13 Phase 2 P2.5 Transform Direct Path

- Command: `npm run bench`
- Note: adds fast-path candidate benchmark groups and enables an internal direct transform path for unobserved simple transforms. Full observer, record behavior, concurrent transform, ordered transform, and special max-in-flight paths continue to use the channel-backed runner.

| Benchmark                                               | ops/sec |  mean ms | samples |
| ------------------------------------------------------- | ------: | -------: | ------: |
| transform: map 10k records                              |       6 |  174.801 |       8 |
| fast-path candidate: transform map 10k records          |       5 |  187.602 |       8 |
| fast-path candidate: transform chain 10k records        |       4 |  246.975 |       8 |
| transform-runner: async map 10k concurrency=1           |       6 |  179.426 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       4 |  273.131 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       3 |  293.419 |       8 |
| csv: decode 10k rows                                    |       5 |  187.160 |       8 |
| batch: write 10k records                                |       8 |  119.087 |       8 |
| pipeline: map+batch+memory 100k records                 |       1 | 1810.754 |       8 |
| fast-path candidate: batch/sink 100k records            |       1 | 1262.411 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |       7 |  140.178 |       8 |

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 88 tests.

## 2026-05-13 Phase 2 P2.5 Batch/Sink Fused Path

- Command: `npm run bench`
- Note: adds an internal fused output path for unobserved batch -> sink execution when `maxWaitMs` is not configured. Full observer and timed batch paths continue to use the channel-backed batch and sink runners.

| Benchmark                                               | ops/sec |  mean ms | samples |
| ------------------------------------------------------- | ------: | -------: | ------: |
| transform: map 10k records                              |       8 |  125.424 |       8 |
| fast-path candidate: transform map 10k records          |       8 |  125.623 |       8 |
| fast-path candidate: transform chain 10k records        |       6 |  176.801 |       8 |
| transform-runner: async map 10k concurrency=1           |       8 |  124.425 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       4 |  227.772 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       4 |  235.833 |       8 |
| csv: decode 10k rows                                    |       7 |  146.784 |       8 |
| batch: write 10k records                                |      13 |   75.669 |       8 |
| pipeline: map+batch+memory 100k records                 |       1 | 1356.189 |       8 |
| fast-path candidate: batch/sink 100k records            |       1 |  779.239 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      11 |   88.175 |       8 |

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 90 tests.

## 2026-05-13 Phase 2 P2.5 Source Direct Path Decision

- Command: `npm run bench`
- Note: P2.5.5 decided not to change the `RuntimeChannel` API yet. The remaining default-path hot boundary was the source channel, so this pass adds an internal direct source path for unobserved runs without record hooks. Full observer and record-hook paths continue to use the source channel runner.

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      12 |  80.879 |       8 |
| fast-path candidate: transform map 10k records          |      11 |  88.705 |       8 |
| fast-path candidate: transform chain 10k records        |       7 | 137.408 |       8 |
| transform-runner: async map 10k concurrency=1           |      12 |  81.024 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       6 | 178.417 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       5 | 185.949 |       8 |
| csv: decode 10k rows                                    |       8 | 130.186 |       8 |
| batch: write 10k records                                |      33 |  30.502 |       8 |
| pipeline: map+batch+memory 100k records                 |       1 | 880.143 |       8 |
| fast-path candidate: batch/sink 100k records            |       3 | 327.689 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      21 |  47.863 |       8 |

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 92 tests.

## 2026-05-13 Phase 2 P2.6 Stability Recheck

- Command: `npm run bench`
- Note: first stability recheck after P2.5.3-P2.5.5 direct paths. Results remain within the P2.6 success range: default 10k map under 100ms and default 100k memory pipeline under 1000ms.

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      12 |  86.088 |       8 |
| fast-path candidate: transform map 10k records          |      12 |  84.106 |       8 |
| fast-path candidate: transform chain 10k records        |       7 | 134.408 |       8 |
| transform-runner: async map 10k concurrency=1           |      11 |  93.566 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       5 | 201.735 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       5 | 198.631 |       8 |
| csv: decode 10k rows                                    |       7 | 135.707 |       8 |
| batch: write 10k records                                |      30 |  32.857 |       8 |
| pipeline: map+batch+memory 100k records                 |       1 | 922.049 |       8 |
| fast-path candidate: batch/sink 100k records            |       3 | 365.384 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      21 |  48.730 |       8 |

## 2026-05-13 Phase 2 P2.6 Codec Direct Path

- Command: `npm run bench`
- Note: adds internal direct decoder/encoder paths for unobserved codec stages. Full observer paths continue to use the channel-backed codec runners, preserving internal channel event visibility.

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      12 |  82.144 |       8 |
| fast-path candidate: transform map 10k records          |      12 |  84.729 |       8 |
| fast-path candidate: transform chain 10k records        |       8 | 132.367 |       8 |
| transform-runner: async map 10k concurrency=1           |      12 |  81.603 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       5 | 187.436 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       5 | 198.307 |       8 |
| channel path: sync map 10k concurrency=8                |       5 | 188.524 |       8 |
| observer: map 10k records full events                   |       2 | 497.663 |       8 |
| behavior: progress map 10k records                      |       4 | 245.812 |       8 |
| csv: decode 10k rows                                    |      13 |  79.935 |       8 |
| codec candidate: csv decode 100k rows                   |       1 | 828.853 |       8 |
| batch: write 10k records                                |      31 |  31.776 |       8 |
| pipeline: map+batch+memory 100k records                 |       1 | 868.535 |       8 |
| fast-path candidate: batch/sink 100k records            |       3 | 327.540 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      22 |  46.308 |       8 |

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 93 tests.

## 2026-05-14 Phase 2 P2.10 Browser Worker Transfer Benchmark

- Command: `npm run bench`
- Note: adds a Node `worker_threads` adapter benchmark for the browser `webWorkerTransform()` protocol so the transfer-list path can be compared without adding browser automation to the benchmark runner. The payload is 200 `ArrayBuffer` records at 256 KiB each by default, with `concurrency: 8` and `maxPending: 16`. Transfer is materially faster for ownership-movable binary payloads, so docs should continue recommending explicit `transfer(input)` when the caller no longer needs the original buffer.

| Benchmark                                       | ops/sec | mean ms | samples |
| ----------------------------------------------- | ------: | ------: | ------: |
| browser worker: ArrayBuffer clone 200x256KiB    |      10 |  99.732 |       8 |
| browser worker: ArrayBuffer transfer 200x256KiB |      17 |  59.172 |       8 |

Quality gate captured before this benchmark pass: `npm run verify` passed with 16 test files and 97 tests.

## 2026-05-14 Phase 2 P2.10 Real Browser Worker Benchmark

- Command: `npm run bench:browser-worker`
- Note: runs a Vite fixture in headless Chrome through the same `webWorkerTransform()` protocol. The default payload is intentionally small so the benchmark is fast and diagnostic rather than a release SLA. Larger browser payloads can be captured with `BROWSER_WORKER_BENCH_COUNT`, `BROWSER_WORKER_BENCH_SIZE`, and `BROWSER_WORKER_BENCH_ITERATIONS`.

| Benchmark                                     | mean ms | samples |
| --------------------------------------------- | ------: | ------: |
| browser worker: ArrayBuffer clone 10x64KiB    |  24.000 |       2 |
| browser worker: ArrayBuffer transfer 10x64KiB |  10.500 |       2 |

Interpretation: the real browser result agrees with the Node worker adapter benchmark. Explicit `transfer(input)` remains the right recommendation for ownership-movable binary payloads.

## 2026-05-13 Phase 2 P2.8 Stability Recheck

- Command: `npm run bench` twice.
- Note: rechecks the completed P2.8 metrics key-cache system after channel, record, stage, batch, and sink metric keys were cached. Results are stable enough to close P2.8: default map remains around 46ms, full observer around 225-234ms, progress behavior around 58-60ms, and 100k memory pipeline around 520ms.

First run:

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      22 |  46.303 |       8 |
| fast-path candidate: transform map 10k records          |      22 |  46.416 |       8 |
| fast-path candidate: transform chain 10k records        |      15 |  67.805 |       8 |
| transform-runner: async map 10k concurrency=1           |      22 |  46.695 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       9 | 112.359 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       8 | 121.273 |       8 |
| channel path: sync map 10k concurrency=8                |       9 | 111.484 |       8 |
| observer: map 10k records full events                   |       4 | 234.376 |       8 |
| observer-cost: map 10k full events type counts          |       4 | 237.697 |       8 |
| observer-cost: map 10k full events async noop           |       4 | 230.224 |       8 |
| channel path: sync map 10k concurrency=8 full events    |       5 | 215.779 |       8 |
| behavior: progress map 10k records                      |      17 |  57.843 |       8 |
| csv: decode 10k rows                                    |      20 |  51.547 |       8 |
| codec candidate: csv decode 100k rows                   |       2 | 524.023 |       8 |
| batch: write 10k records                                |      37 |  27.229 |       8 |
| pipeline: map+batch+memory 100k records                 |       2 | 520.356 |       8 |
| fast-path candidate: batch/sink 100k records            |       4 | 261.389 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      25 |  40.259 |       8 |

Second run:

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      22 |  46.512 |       8 |
| fast-path candidate: transform map 10k records          |      22 |  46.388 |       8 |
| fast-path candidate: transform chain 10k records        |      15 |  68.752 |       8 |
| transform-runner: async map 10k concurrency=1           |      21 |  47.087 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |      10 | 105.267 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       9 | 111.110 |       8 |
| channel path: sync map 10k concurrency=8                |      10 | 101.614 |       8 |
| observer: map 10k records full events                   |       4 | 224.585 |       8 |
| observer-cost: map 10k full events type counts          |       4 | 243.921 |       8 |
| observer-cost: map 10k full events async noop           |       4 | 230.307 |       8 |
| channel path: sync map 10k concurrency=8 full events    |       5 | 211.664 |       8 |
| behavior: progress map 10k records                      |      17 |  59.936 |       8 |
| csv: decode 10k rows                                    |      19 |  52.497 |       8 |
| codec candidate: csv decode 100k rows                   |       2 | 522.423 |       8 |
| batch: write 10k records                                |      39 |  25.726 |       8 |
| pipeline: map+batch+memory 100k records                 |       2 | 521.592 |       8 |
| fast-path candidate: batch/sink 100k records            |       4 | 262.568 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      27 |  38.320 |       8 |

## 2026-05-13 Phase 2 P2.8 Batch/Sink Metrics Key Cache

- Command: `npm run bench`
- Note: completes the shared metrics key-cache pass by adding batch flush and sink write metric keys, plus a direct `applyBatchMetrics()` helper for unobserved batch notifications. This keeps metrics behavior and observer events unchanged.

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      21 |  48.022 |       8 |
| fast-path candidate: transform map 10k records          |      21 |  47.211 |       8 |
| fast-path candidate: transform chain 10k records        |      14 |  71.570 |       8 |
| transform-runner: async map 10k concurrency=1           |      21 |  47.883 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       9 | 112.787 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       8 | 123.839 |       8 |
| channel path: sync map 10k concurrency=8                |       9 | 118.005 |       8 |
| observer: map 10k records full events                   |       4 | 234.760 |       8 |
| observer-cost: map 10k full events type counts          |       4 | 248.040 |       8 |
| observer-cost: map 10k full events async noop           |       4 | 248.802 |       8 |
| channel path: sync map 10k concurrency=8 full events    |       4 | 232.289 |       8 |
| behavior: progress map 10k records                      |      16 |  62.758 |       8 |
| csv: decode 10k rows                                    |      19 |  52.425 |       8 |
| codec candidate: csv decode 100k rows                   |       2 | 520.228 |       8 |
| batch: write 10k records                                |      37 |  27.183 |       8 |
| pipeline: map+batch+memory 100k records                 |       2 | 541.737 |       8 |
| fast-path candidate: batch/sink 100k records            |       4 | 274.305 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      25 |  39.866 |       8 |

Targeted gate captured after this pass:

- `npm test -- --run packages/core/src/metrics.test.ts packages/core/src/pipeline.test.ts packages/core/src/runners.test.ts packages/core/src/behaviors.test.ts` passed with 4 test files and 51 tests.
- `npm run typecheck` passed.
- `npm run format:check` passed.

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 93 tests.

## 2026-05-13 Phase 2 P2.8 Record/Stage Metrics Key Cache

- Command: `npm run bench`
- Note: extends the P2.8 hot-path key-cache approach from channel metrics to record and stage metrics. This is a shared metrics optimization rather than a second executor path: event semantics, observer dispatch, and runner/channel behavior are unchanged.

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      21 |  47.631 |       8 |
| fast-path candidate: transform map 10k records          |      21 |  46.606 |       8 |
| fast-path candidate: transform chain 10k records        |      15 |  68.300 |       8 |
| transform-runner: async map 10k concurrency=1           |      21 |  47.229 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       9 | 109.593 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       9 | 117.010 |       8 |
| channel path: sync map 10k concurrency=8                |       9 | 107.365 |       8 |
| observer: map 10k records full events                   |       4 | 241.747 |       8 |
| observer-cost: map 10k full events type counts          |       4 | 247.342 |       8 |
| observer-cost: map 10k full events async noop           |       4 | 253.774 |       8 |
| channel path: sync map 10k concurrency=8 full events    |       4 | 225.187 |       8 |
| behavior: progress map 10k records                      |      17 |  58.826 |       8 |
| csv: decode 10k rows                                    |      20 |  50.023 |       8 |
| codec candidate: csv decode 100k rows                   |       2 | 538.657 |       8 |
| batch: write 10k records                                |      37 |  26.796 |       8 |
| pipeline: map+batch+memory 100k records                 |       2 | 520.134 |       8 |
| fast-path candidate: batch/sink 100k records            |       4 | 277.287 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      23 |  44.770 |       8 |

Targeted gate captured after this pass:

- `npm test -- --run packages/core/src/metrics.test.ts packages/core/src/pipeline.test.ts packages/core/src/behaviors.test.ts packages/core/src/runners.test.ts` passed with 4 test files and 51 tests.
- `npm run typecheck` passed.
- `npm run format:check` passed.

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 93 tests.

## 2026-05-13 Phase 2 P2.7 Behavior Direct Path

- Command: `npm run bench`
- Note: allows record behavior scenarios, such as `progressBehavior`, to use the direct source and direct transform paths when full event observation is not enabled. Full observer paths continue to use channel-backed execution.

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      12 |  82.628 |       8 |
| fast-path candidate: transform map 10k records          |      12 |  81.306 |       8 |
| fast-path candidate: transform chain 10k records        |       8 | 129.141 |       8 |
| transform-runner: async map 10k concurrency=1           |      12 |  80.228 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       5 | 196.774 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       5 | 205.439 |       8 |
| channel path: sync map 10k concurrency=8                |       5 | 194.408 |       8 |
| observer: map 10k records full events                   |       2 | 519.109 |       8 |
| behavior: progress map 10k records                      |      10 |  95.869 |       8 |
| csv: decode 10k rows                                    |      13 |  79.349 |       8 |
| codec candidate: csv decode 100k rows                   |       1 | 825.373 |       8 |
| batch: write 10k records                                |      31 |  32.279 |       8 |
| pipeline: map+batch+memory 100k records                 |       1 | 888.312 |       8 |
| fast-path candidate: batch/sink 100k records            |       3 | 337.204 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      22 |  44.589 |       8 |

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 93 tests.

## 2026-05-13 Phase 2 P2.8 Observer Cost Model Setup

- Command: `npm run bench`
- Note: adds cost-model benchmark groups for full observer paths. This pass does not change runtime behavior. The first read is that async noop observer dispatch is not materially slower than a sync noop observer; the full observer cost is dominated by high event volume, especially channel push/pull/wait events in the channel-backed path.

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      11 |  93.353 |       8 |
| fast-path candidate: transform map 10k records          |      11 |  93.579 |       8 |
| fast-path candidate: transform chain 10k records        |       7 | 145.397 |       8 |
| transform-runner: async map 10k concurrency=1           |      11 |  91.883 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       5 | 200.094 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       5 | 197.411 |       8 |
| channel path: sync map 10k concurrency=8                |       5 | 183.441 |       8 |
| observer: map 10k records full events                   |       2 | 488.463 |       8 |
| observer-cost: map 10k full events type counts          |       2 | 516.749 |       8 |
| observer-cost: map 10k full events async noop           |       2 | 486.630 |       8 |
| channel path: sync map 10k concurrency=8 full events    |       2 | 448.522 |       8 |
| behavior: progress map 10k records                      |      11 |  95.091 |       8 |
| csv: decode 10k rows                                    |      13 |  79.769 |       8 |
| codec candidate: csv decode 100k rows                   |       1 | 833.140 |       8 |
| batch: write 10k records                                |      31 |  32.124 |       8 |
| pipeline: map+batch+memory 100k records                 |       1 | 892.867 |       8 |
| fast-path candidate: batch/sink 100k records            |       3 | 342.222 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      22 |  46.271 |       8 |

Full observer event distribution for `observer: map 10k records full events`:

| Event type        | Count |
| ----------------- | ----: |
| channel.pull      | 40010 |
| channel.push      | 40010 |
| channel.wait      | 39973 |
| record            | 20000 |
| stage.item.end    | 10000 |
| stage.item.start  | 10000 |
| batch             |    10 |
| batch.flush.end   |    10 |
| batch.flush.start |    10 |
| sink.write.end    |    10 |
| sink.write.start  |    10 |
| stage.end         |     3 |
| stage.start       |     3 |
| finish            |     1 |
| start             |     1 |

## 2026-05-13 Phase 2 P2.8 Channel Metrics Key Cache

- Command: `npm run bench`
- Note: keeps full observer event semantics intact while caching channel metric keys in `metrics.ts` and using a direct no-observer channel wait metrics path in `channel.ts`. The full observer event distribution is unchanged from the cost-model setup; the improvement comes from avoiding repeated channel metric string work and avoiding event construction for unobserved waits.

| Benchmark                                               | ops/sec | mean ms | samples |
| ------------------------------------------------------- | ------: | ------: | ------: |
| transform: map 10k records                              |      11 |  87.569 |       8 |
| fast-path candidate: transform map 10k records          |      12 |  84.744 |       8 |
| fast-path candidate: transform chain 10k records        |       7 | 136.248 |       8 |
| transform-runner: async map 10k concurrency=1           |      11 |  87.322 |       8 |
| transform-runner: async map 10k concurrency=8 unordered |       7 | 143.600 |       8 |
| transform-runner: async map 10k concurrency=8 ordered   |       6 | 154.852 |       8 |
| channel path: sync map 10k concurrency=8                |       7 | 145.656 |       8 |
| observer: map 10k records full events                   |       4 | 276.390 |       8 |
| observer-cost: map 10k full events type counts          |       4 | 284.554 |       8 |
| observer-cost: map 10k full events async noop           |       4 | 271.144 |       8 |
| channel path: sync map 10k concurrency=8 full events    |       4 | 259.540 |       8 |
| behavior: progress map 10k records                      |      10 |  98.719 |       8 |
| csv: decode 10k rows                                    |      12 |  82.608 |       8 |
| codec candidate: csv decode 100k rows                   |       1 | 842.222 |       8 |
| batch: write 10k records                                |      29 |  33.946 |       8 |
| pipeline: map+batch+memory 100k records                 |       1 | 895.480 |       8 |
| fast-path candidate: batch/sink 100k records            |       3 | 334.797 |       8 |
| pipeline: slow sink 10k records 1ms/batch               |      22 |  46.769 |       8 |

Targeted gate captured after this pass:

- `npm test -- --run packages/core/src/channel.test.ts packages/core/src/metrics.test.ts packages/core/src/pipeline.test.ts packages/core/src/runners.test.ts` passed with 4 test files and 51 tests.
- `npm run typecheck` passed.
- `npm run format:check` passed.

Quality gate captured after this pass: `npm run verify` passed with 15 test files and 93 tests.
