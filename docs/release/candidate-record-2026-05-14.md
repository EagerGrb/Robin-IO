# Release Candidate Record 2026-05-14

This is a dry-run release candidate rehearsal record. No npm publish was attempted.

## Candidate

- Version: `0.1.0` workspace rehearsal
- Date: 2026-05-14
- Branch or commit: local workspace, git status unavailable from this environment
- Release owner: Codex rehearsal
- Changeset summary: blocked, see Required Gates
- Compatibility notes: release readiness RR1-RR7 governance checks only; no runtime behavior change in this rehearsal step

## Environment

- OS: Microsoft Windows Server 2022 Standard
- Node: v24.13.0
- npm: 11.6.2
- Browser: auto-discovered Chrome/Edge compatible browser through browser worker harness
- `CHROME_PATH`: not set
- Notes: browser worker smoke and benchmark succeeded without explicit `CHROME_PATH`

## Required Gates

| Gate                       | Command                          | Result  | Notes                                                                                              |
| -------------------------- | -------------------------------- | ------- | -------------------------------------------------------------------------------------------------- |
| Full verification          | `npm run verify`                 | Passed  | 16 test files and 97 tests passed; Node dist smoke, browser build, and browser worker smoke passed |
| Package/API/release checks | `npm run package:check`          | Passed  | 12 package export checks passed; public API surface and release docs checks passed                 |
| API surface drift          | `npm run api:check`              | Passed  | Covered through `package:check` in this rehearsal                                                  |
| Changeset config           | `npm run changeset:check-config` | Passed  | Covered through `verify`                                                                           |
| Changeset release status   | `npm run changeset:status`       | Blocked | Failed to find where HEAD diverged from `main`; local `main` is missing or not synced              |
| Node dist smoke            | `npm run smoke:node-dist`        | Passed  | Covered through `verify`                                                                           |
| Browser worker smoke       | `npm run smoke:browser-worker`   | Passed  | Covered through `verify`                                                                           |

## Benchmark Evidence

This rehearsal records current values. Final release approval should compare against the active baseline in the release PR.

| Benchmark group                  | Command         | Result     | Baseline comparison | Decision |
| -------------------------------- | --------------- | ---------- | ------------------- | -------- |
| Transform map 10k                | `npm run bench` | 49.11 ms   | Not compared        | Observe  |
| Full observer 10k                | `npm run bench` | 233.687 ms | Not compared        | Observe  |
| Progress behavior 10k            | `npm run bench` | 59.406 ms  | Not compared        | Observe  |
| CSV decode 100k                  | `npm run bench` | 537.26 ms  | Not compared        | Observe  |
| Memory pipeline 100k             | `npm run bench` | 518.31 ms  | Not compared        | Observe  |
| Batch/sink fast path 100k        | `npm run bench` | 266.99 ms  | Not compared        | Observe  |
| Node adapter clone 200x256KiB    | `npm run bench` | 95.776 ms  | Not compared        | Passed   |
| Node adapter transfer 200x256KiB | `npm run bench` | 57.82 ms   | Not compared        | Passed   |

For browser worker performance, transfer should remain faster than clone in the default real-browser case.

| Browser worker case | Command                        | Clone   | Transfer | Decision |
| ------------------- | ------------------------------ | ------- | -------- | -------- |
| Default fixture     | `npm run bench:browser-worker` | 28.5 ms | 9 ms     | Passed   |

## Release Decision

- Approve release candidate: No
- Blockers: `npm run changeset:status` requires a local `main` branch synced with remote
- Follow-up issues: rerun `changeset:status` in a proper git release branch before approving a beta release
- Rollback plan: no version/changelog or publish changes were made during this rehearsal
- Publish notes: npm publishing remains intentionally manual and was not exercised

## Links

- Release readiness: `docs/release/readiness.md`
- Release rehearsal: `docs/release/rehearsal.md`
- CI matrix: `docs/release/ci-matrix.md`
- API surface: `docs/release/api-surface.core.json`
