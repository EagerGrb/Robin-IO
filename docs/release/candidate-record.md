# Release Candidate Record

Use this template for every beta release candidate. Keep the completed record in the release PR or release branch notes.

## Candidate

- Version:
- Date:
- Branch or commit:
- Release owner:
- Changeset summary:
- Compatibility notes:

## Environment

- OS:
- Node:
- npm:
- Browser:
- `CHROME_PATH`:
- Notes:

## Required Gates

Record the command result and any relevant output summary.

| Gate                       | Command                          | Result | Notes |
| -------------------------- | -------------------------------- | ------ | ----- |
| Full verification          | `npm run verify`                 |        |       |
| Package/API/release checks | `npm run package:check`          |        |       |
| API surface drift          | `npm run api:check`              |        |       |
| Changeset config           | `npm run changeset:check-config` |        |       |
| Changeset release status   | `npm run changeset:status`       |        |       |
| Node dist smoke            | `npm run smoke:node-dist`        |        |       |
| Browser worker smoke       | `npm run smoke:browser-worker`   |        |       |

## Benchmark Evidence

Compare `npm run bench` with `docs/architecture/performance-baseline.md`. Explain any sustained regression above 10%.

| Benchmark group | Command         | Result | Baseline comparison | Decision |
| --------------- | --------------- | ------ | ------------------- | -------- |
| Runtime/core    | `npm run bench` |        |                     |          |

For browser worker performance, transfer should remain faster than clone in the default real-browser case.

| Browser worker case | Command                        | Clone | Transfer | Decision |
| ------------------- | ------------------------------ | ----- | -------- | -------- |
| Default fixture     | `npm run bench:browser-worker` |       |          |          |

## Release Decision

- Approve release candidate:
- Blockers:
- Follow-up issues:
- Rollback plan:
- Publish notes:

## Links

- Release readiness: `docs/release/readiness.md`
- Release rehearsal: `docs/release/rehearsal.md`
- CI matrix: `docs/release/ci-matrix.md`
- API surface: `docs/release/api-surface.core.json`
