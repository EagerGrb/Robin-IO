# Release Candidate Record / 发布候选记录

Use this template for every beta release candidate. Keep the completed record in the release PR or release branch notes.

每个 beta release candidate 都使用此模板。完成后的记录保存在 release PR 或 release 分支说明中。

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

记录命令结果和关键输出摘要。

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

将 `npm run bench` 与 `docs/architecture/performance-baseline.md` 对比。持续超过 10% 的回归必须解释。

| Benchmark group | Command         | Result | Baseline comparison | Decision |
| --------------- | --------------- | ------ | ------------------- | -------- |
| Runtime/core    | `npm run bench` |        |                     |          |

For browser worker performance, transfer should remain faster than clone in the default real-browser case.

对于 browser worker 性能，默认真实浏览器场景中 transfer 应继续快于 clone。

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
