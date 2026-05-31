# Release Rehearsal

This document defines the dry-run sequence before a beta publish. It is intentionally conservative: the goal is to catch package metadata, API surface, browser-worker, and changeset issues before touching the npm registry.

## Preconditions

- Working tree reviewed; unrelated local changes are understood.
- Chrome or Edge is available, or `CHROME_PATH` points to a compatible browser.
- `main` is available locally when running `changeset:status`.
- No example workspace should be publishable.
- Every public package under `packages/*` should be eligible for changesets.
- The release candidate environment matches `docs/release/ci-matrix.md`, or deviations are documented in the release PR.

## Dry Run

Run:

```bash
npm run verify
npm run api:check
npm run bench
npm run bench:browser-worker
npm run changeset:check-config
npm run changeset:status
```

Expected outcome:

- `verify` passes with tests, typecheck, build, package export checks, Node dist smoke, browser example build, and browser worker smoke.
- `api:check` confirms root value exports match the documented API surface.
- `bench` is compared with `docs/architecture/performance-baseline.md`.
- `bench:browser-worker` confirms transfer remains faster than clone for the default browser worker case.
- `changeset:check-config` confirms examples are ignored and publishable packages are not ignored.
- `changeset:status` shows intended package changes only.

## Version Rehearsal

Only run this on a release branch or disposable rehearsal branch:

```bash
npm run changeset:version
npm run verify
npm run package:check
```

Review generated changelogs and version bumps before committing.

## Publish Readiness

A beta publish is ready only when:

- all dry-run commands pass;
- intended package list is clear;
- public API surface changes are intentional;
- migration notes exist for breaking changes;
- benchmark deltas above 10% are explained;
- `docs/release/candidate-record.md` is completed for the release candidate;
- rollback plan is written in the release PR.

## Rollback Rehearsal

Before the first public beta publish, rehearse reverting generated version/changelog changes on a disposable branch. Do not rehearse npm unpublish; prefer follow-up patch releases after a real publish.
