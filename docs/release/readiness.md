# Release Readiness

This project is still pre-1.0. Release readiness means a package can be published for controlled beta use with explicit compatibility limits.

## Public API

The stable public entry for each package is its root export, for example:

```ts
import { pipeline } from "@robbin-io/core"
import { webWorkerTransform } from "@robbin-io/browser"
```

Pre-1.0 compatibility rules:

- Root-exported functions, classes, interfaces, and constants are public unless documented as experimental.
- Package subpaths named `internal` are intentionally unstable and are not application-facing public API.
- Public APIs can still change before 1.0, but breaking changes require a changeset and migration notes.
- Core framework error categories should use `CORE_RUNTIME_ERROR_CODES` when possible.

`docs/release/api-surface.core.json` records the current `@robbin-io/core` root value exports. `npm run api:check` fails when root value exports drift without an intentional update.

`@robbin-io/core/internal` is a separate implementation entry for framework internals. Its package export is checked so builds do not drift, but its value exports are not part of the stable root API allowlist.

## Experimental Surface

The following surfaces are public for beta feedback but not yet long-term stable:

- detailed runtime event payload shapes beyond `type`, `timestamp`, `stage`, and `metadata`;
- metric key names for internal stage/channel/batch/sink measurements;
- browser worker benchmark scripts and fixture packages;
- internal scheduling behavior used to choose direct, fused, or channel-backed execution paths;
- progress semantics around physical chunks vs logical rows after encoder chunk aggregation.

## Internal Surface

Do not depend on files under package `src/` or `dist/` paths directly. Import only through package roots.

Framework-owned packages may import from documented internal subpaths such as `@robbin-io/core/internal` when they need execution internals. Application code should not depend on these internal subpaths.

Implementation details include:

- scheduler internals;
- runner classes;
- channel implementation details;
- `@robbin-io/core/internal` value exports;
- benchmark fixtures;
- smoke-test harnesses.

## Release Gate

Before publishing a beta release:

```bash
npm run verify
npm run api:check
npm run bench
npm run bench:browser-worker
npm run package:check
npm run package:tarballs:check
npm run smoke:packed-consumer
npm run smoke:browser-matrix
npm run release:publish:dry-run
npm run changeset:check-config
```

See `docs/release/ci-matrix.md` for the required PR gate, release candidate gate, and environment matrix.

Use `docs/release/candidate-record.md` to record release candidate evidence before approval.

For environments without Chrome or Edge, document that `smoke:browser-worker` and `bench:browser-worker` require `CHROME_PATH`.

## CI And npm Publish Loop

The repository includes:

- `.github/workflows/ci.yml` for PR/push verification plus Playwright browser matrix.
- `.github/workflows/release.yml` for manual release gates and optional npm publish.
- `npm run release:publish:dry-run` for local or CI publish simulation.
- `npm run release:publish` for actual package publishing from CI.

Real publishing is intentionally guarded:

- use the `main` branch;
- configure npm Trusted Publishing or repository secret `NPM_TOKEN`;
- configure the `npm` environment if environment approvals are required;
- run the release workflow with `publish: true`;
- do not publish examples, only packages under `packages/*`.
