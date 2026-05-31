# CI Matrix / CI 验证矩阵

This document defines the minimum validation matrix for beta releases. It separates fast PR gates from release-candidate gates so performance and browser checks stay useful without making everyday development brittle.

本文档定�?beta release 的最低验证矩阵。它把快�?PR 门槛�?release candidate 门槛分开，避免性能和浏览器检查拖垮日常开发�?
## Required PR Gate / 必需 PR 门槛

Run on every change that touches packages, examples, scripts, or docs used by release:

凡修�?packages、examples、scripts 或发布相�?docs，均需运行�?
```bash
npm run verify
```

`verify` covers:

`verify` 覆盖�?
- formatting;
- Vitest unit/integration tests;
- TypeScript project references;
- package build;
- package metadata and exports;
- public API surface check through `package:check`;
- changeset config check;
- Node dist smoke;
- clean install packed tarball consumer smoke;
- browser CSV preview build;
- real headless Chrome browser CSV, CSV+Worker, WritableStream, and Worker smoke.

CI also runs a separate browser matrix job:

```bash
npm run smoke:browser-matrix
```

The matrix installs Playwright browsers and runs the browser smoke fixtures on Chromium, Firefox, and WebKit.

## Release Candidate Gate / 发布候选门�?
Run before creating or approving a beta release branch:

创建或批�?beta release 分支前运行：

```bash
npm run verify
npm run api:check
npm run bench
npm run bench:browser-worker
npm run smoke:browser-matrix
npm run changeset:check-config
npm run changeset:status
npm run release:publish:dry-run
```

Record the results in `docs/release/candidate-record.md` or the release PR using that template.

使用 `docs/release/candidate-record.md` 模板�?release PR 中记录结果�?
Interpretation rules:

判读规则�?
- `bench` should be compared with `docs/architecture/performance-baseline.md`.
- Any sustained regression above 10% in default map, progress behavior, CSV decode, or 100k memory pipeline needs a release note or fix.
- `bench:browser-worker` is directional; transfer should remain faster than clone for the default browser worker case.
- Browser checks require Chrome or Edge. Set `CHROME_PATH` when auto-discovery is not available.

## Environment Matrix / 环境矩阵

Minimum local release environment:

最低本地发布环境：

| Area    | Required       | Notes                                                                       |
| ------- | -------------- | --------------------------------------------------------------------------- |
| Node    | 24.x           | Packages declare `engines.node: >=24`; current validation uses Node 24.13.0 |
| npm     | 11.x           | Current validation uses npm 11.6.2                                          |
| OS      | Windows        | Current active validation environment                                       |
| Browser | Chrome or Edge | Required for local CDP smokes and `bench:browser-worker`                    |

Target CI expansion before public 1.0:

公开 1.0 前建议扩展的 CI�?
| Area            | Target                                                  |
| --------------- | ------------------------------------------------------- |
| Node            | Latest 24.x and next active LTS                         |
| OS              | Windows and Linux                                       |
| Browser         | Chromium, Firefox, and WebKit via Playwright            |
| Package manager | npm only unless pnpm/yarn support is explicitly adopted |

## Browser Worker Checks / Browser Worker 检�?
`smoke:browser-worker` is correctness-oriented and is part of `verify`.

`smoke:browser-worker` 面向正确性，已纳�?`verify`�?
It checks:

- Vite build;
- real module Worker startup;
- `webWorkerTransform()` request/response;
- pipeline integration;
- `transfer(input)` detached buffer behavior.

`bench:browser-worker` is performance-oriented and should not be part of ordinary PR gates. It records clone vs transfer timing in a real headless Chrome session.

`bench:browser-worker` 面向性能，不应放入普�?PR 门槛。它记录真实 headless Chrome �?clone �?transfer 的耗时对比�?
## Failure Handling / 失败处理

- Formatting, typecheck, tests, package checks, API surface checks, and smoke tests are blocking.
- Benchmark regressions are blocking only when sustained and unexplained.
- Browser auto-discovery failures may be handled by setting `CHROME_PATH`; do not skip browser worker smoke for release candidates.
- If a release candidate fails after versioning, revert generated version/changelog changes and rerun the full release candidate gate.

## Release Workflow

`.github/workflows/release.yml` defines a manual release workflow:

- `release-gate` runs `verify`, the Playwright browser matrix, benchmarks, Changesets status, and `release:publish:dry-run`.
- `publish` runs only on `main` when the workflow input `publish` is true.
- Real npm publish requires repository secret `NPM_TOKEN` and environment `npm`.
- `scripts/publish-packages.mjs` publishes only `packages/*`, skips private examples, and publishes internal `@robbin-io/*` dependencies before dependents.

The workflow is intentionally manual. Do not publish from a local machine unless the same release gate has passed on CI.
