# Release Rehearsal / 发布演练

This document defines the dry-run sequence before a beta publish. It is intentionally conservative: the goal is to catch package metadata, API surface, browser-worker, and changeset issues before touching the npm registry.

本文档定义 beta 发布前的 dry-run 顺序。流程刻意保守：目标是在接触 npm registry 前发现 package metadata、API surface、browser-worker 和 changeset 问题。

## Preconditions / 前置条件

- Working tree reviewed; unrelated local changes are understood.
- Chrome or Edge is available, or `CHROME_PATH` points to a compatible browser.
- `main` is available locally when running `changeset:status`.
- No example workspace should be publishable.
- Every public package under `packages/*` should be eligible for changesets.
- The release candidate environment matches `docs/release/ci-matrix.md`, or deviations are documented in the release PR.

## Dry Run / 干跑流程

Run:

运行：

```bash
npm run verify
npm run api:check
npm run bench
npm run bench:browser-worker
npm run changeset:check-config
npm run changeset:status
```

Expected outcome:

预期结果：

- `verify` passes with tests, typecheck, build, package export checks, Node dist smoke, browser example build, and browser worker smoke.
- `api:check` confirms root value exports match the documented API surface.
- `bench` is compared with `docs/architecture/performance-baseline.md`.
- `bench:browser-worker` confirms transfer remains faster than clone for the default browser worker case.
- `changeset:check-config` confirms examples are ignored and publishable packages are not ignored.
- `changeset:status` shows intended package changes only.

## Version Rehearsal / 版本演练

Only run this on a release branch or disposable rehearsal branch:

仅在 release 分支或一次性演练分支运行：

```bash
npm run changeset:version
npm run verify
npm run package:check
```

Review generated changelogs and version bumps before committing.

提交前检查生成的 changelog 和版本号变更。

## Publish Readiness / 发布就绪判断

A beta publish is ready only when:

只有满足以下条件时才进入 beta publish：

- all dry-run commands pass;
- intended package list is clear;
- public API surface changes are intentional;
- migration notes exist for breaking changes;
- benchmark deltas above 10% are explained;
- `docs/release/candidate-record.md` is completed for the release candidate;
- rollback plan is written in the release PR.

## Rollback Rehearsal / 回滚演练

Before first public beta publish, rehearse reverting generated version/changelog changes on a disposable branch. Do not rehearse npm unpublish; prefer follow-up patch releases after a real publish.

首次 public beta 发布前，在一次性分支演练回滚版本号/changelog 变更。不要演练 npm unpublish；真实发布后优先使用后续 patch release 修复。
