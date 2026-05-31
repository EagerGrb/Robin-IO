# CI Matrix

This document defines the minimum validation matrix for beta releases. It separates fast PR gates from release-candidate gates so performance and browser checks stay useful without making everyday development brittle.

## Required PR Gate

Run on every change that touches packages, examples, scripts, or docs used by release:

```bash
npm run verify
```

`verify` covers:

- formatting;
- Vitest unit and integration tests;
- TypeScript project references;
- package build;
- package metadata and exports;
- public API surface checks through `package:check`;
- changeset config checks;
- Node dist smoke;
- clean install packed tarball consumer smoke;
- browser CSV preview build;
- real headless Chrome browser CSV, CSV+Worker, WritableStream, and Worker smoke.

CI also runs a separate browser matrix job:

```bash
npm run smoke:browser-matrix
```

The matrix installs Playwright browsers and runs browser smoke fixtures on Chromium, Firefox, and WebKit.

## Release Candidate Gate

Run before creating or approving a beta release branch:

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

Record the results in `docs/release/candidate-record.md` or in the release PR using that template.

Interpretation rules:

- `bench` should be compared with `docs/architecture/performance-baseline.md`.
- Any sustained regression above 10% in default map, progress behavior, CSV decode, or 100k memory pipeline needs a release note or fix.
- `bench:browser-worker` is directional; transfer should remain faster than clone for the default browser worker case.
- Browser checks require Chrome or Edge. Set `CHROME_PATH` when auto-discovery is not available.

## Environment Matrix

Minimum local release environment:

| Area    | Required       | Notes                                                                       |
| ------- | -------------- | --------------------------------------------------------------------------- |
| Node    | 24.x           | Packages declare `engines.node: >=24`; current validation uses Node 24.13.0 |
| npm     | 11.x           | Current validation uses npm 11.6.2                                          |
| OS      | Windows        | Current active validation environment                                       |
| Browser | Chrome or Edge | Required for local CDP smokes and `bench:browser-worker`                    |

Target CI expansion before public 1.0:

| Area            | Target                                                  |
| --------------- | ------------------------------------------------------- |
| Node            | Latest 24.x and next active LTS                         |
| OS              | Windows and Linux                                       |
| Browser         | Chromium, Firefox, and WebKit via Playwright            |
| Package manager | npm only unless pnpm/yarn support is explicitly adopted |

## Browser Worker Checks

`smoke:browser-worker` is correctness-oriented and is part of `verify`.

It checks:

- Vite build;
- real module Worker startup;
- `webWorkerTransform()` request/response;
- pipeline integration;
- `transfer(input)` detached buffer behavior.

`bench:browser-worker` is performance-oriented and should not be part of ordinary PR gates. It records clone vs transfer timing in a real headless Chrome session.

## Failure Handling

- Formatting, typecheck, tests, package checks, API surface checks, and smoke tests are blocking.
- Benchmark regressions are blocking only when sustained and unexplained.
- Browser auto-discovery failures may be handled by setting `CHROME_PATH`; do not skip browser worker smoke for release candidates.
- If a release candidate fails after versioning, revert generated version/changelog changes and rerun the full release candidate gate.

## Release Workflow

`.github/workflows/release.yml` defines a manual release workflow:

- `release-gate` runs `verify`, the Playwright browser matrix, benchmarks, Changesets status, and `release:publish:dry-run`.
- `publish` runs only on `main` when the workflow input `publish` is true.
- Real npm publish requires npm Trusted Publishing or repository secret `NPM_TOKEN`.
- `scripts/publish-packages.mjs` publishes only `packages/*`, skips private examples, and publishes internal `@robbin-io/*` dependencies before dependents.

The workflow is intentionally manual. Do not publish from a local machine unless the same release gate has passed on CI.
