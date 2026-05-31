# Performance Baseline

Updated: 2026-05-13

This document records the current Phase 2 performance baseline after the P2.8 metrics key cache pass. It is intended as a stable reference for later optimization work, not as a hard release SLA.

## Current Baseline

Source: two consecutive `npm run bench` runs after P2.8.9.

| Benchmark                                               | Stable mean ms |
| ------------------------------------------------------- | -------------: |
| transform: map 10k records                              |      46.3-46.5 |
| fast-path candidate: transform map 10k records          |           46.4 |
| fast-path candidate: transform chain 10k records        |      67.8-68.8 |
| transform-runner: async map 10k concurrency=1           |      46.7-47.1 |
| transform-runner: async map 10k concurrency=8 unordered |    105.3-112.4 |
| transform-runner: async map 10k concurrency=8 ordered   |    111.1-121.3 |
| channel path: sync map 10k concurrency=8                |    101.6-111.5 |
| observer: map 10k records full events                   |    224.6-234.4 |
| channel path: sync map 10k concurrency=8 full events    |    211.7-215.8 |
| behavior: progress map 10k records                      |      57.8-59.9 |
| csv: decode 10k rows                                    |      51.5-52.5 |
| codec candidate: csv decode 100k rows                   |    522.4-524.0 |
| batch: write 10k records                                |      25.7-27.2 |
| pipeline: map+batch+memory 100k records                 |    520.4-521.6 |
| fast-path candidate: batch/sink 100k records            |    261.4-262.6 |
| pipeline: slow sink 10k records 1ms/batch               |      38.3-40.3 |

## Interpretation

- Default unobserved memory pipelines are now in the intended high-performance range for this phase.
- Full observer remains intentionally more expensive because it preserves complete channel, record, stage, batch, and sink events.
- `progressBehavior` is close to the default fast path because it uses record/batch behavior notifications without requiring full channel event observation.
- Concurrent transform paths still use the shared runner/channel execution model. A separate concurrent direct executor was evaluated during P2.8 and rejected because the measured gain was too small for the maintenance cost.

## Guardrails

Future performance work should follow these constraints:

- Do not introduce a second executor for complex transform semantics unless benchmark gains are large and correctness tests cover abort, fail-fast, skip-and-collect, ordering, and backpressure.
- Keep full observer event semantics intact unless a new explicit public observability mode is designed.
- Prefer shared runtime or metrics hot-path improvements over special-case benchmark-only branches.
- Treat a sustained regression above 10% in the default map, progress behavior, CSV decode, or 100k memory pipeline benchmarks as requiring an explanation before release.

## Next Candidates

Likely follow-up areas:

- Runner/channel internal lightening for complex paths.
- Browser worker transform integration coverage now includes a real headless Chrome smoke test and a real browser worker benchmark. P2.10 benchmark data shows explicit `transfer(input)` is materially faster for ownership-movable `ArrayBuffer` payloads than structured clone on the same `webWorkerTransform()` protocol.
- Production observability guidance that explains default, behavior-only, and full observer cost profiles.
