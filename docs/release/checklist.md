# Release Checklist / 发布检查清单

This project uses npm workspaces and Changesets. The root package is private; publishable packages live under `packages/*`.

本项目使用 npm workspaces 和 Changesets。根 package 是 private；可发布 package 位于 `packages/*`。

## Before Release / 发布前

Run the full local quality gate:

运行完整本地质量门：

```bash
npm run verify
npm run bench
```

Check package metadata and exports:

检查 package 元数据和导出：

```bash
npm run package:check
npm run api:check
npm run smoke:node-dist
npm run smoke:browser-worker
```

Check Changesets configuration:

检查 Changesets 配置：

```bash
npm run changeset:check-config
```

Review release readiness policy:

检查发布就绪策略：

```bash
docs/release/deployment-and-growth-plan.md
docs/release/readiness.md
docs/release/rehearsal.md
docs/release/ci-matrix.md
docs/release/candidate-record.md
```

Run the real browser worker benchmark when Chrome or Edge is available:

如果环境有 Chrome 或 Edge，运行真实浏览器 worker benchmark：

```bash
npm run bench:browser-worker
```

## Create Release Notes / 创建发布说明

Create a changeset for user-visible package changes:

为用户可见的 package 变更创建 changeset：

```bash
npm run changeset
```

Review pending release status in a git branch with `main` available:

在可访问 `main` 分支的 git 分支中检查发布状态：

```bash
npm run changeset:status
```

Create a release candidate record before approval:

批准前创建 release candidate 记录：

```bash
docs/release/candidate-record.md
```

## Version Packages / 更新版本

Apply version bumps and changelog updates:

应用版本号和 changelog 更新：

```bash
npm run changeset:version
npm run verify
```

## Publish / 发布

Publishing is intentionally not automated yet. Before adding `changeset publish`, confirm npm access, provenance requirements, package names, and release permissions.

发布目前刻意没有自动化。加入 `changeset publish` 前，需要确认 npm 权限、provenance 要求、package 名称和发布权限。

## Rollback / 回滚

If a release fails before publishing, revert the version/changelog commit and rerun:

如果发布前失败，回滚版本号/changelog 提交后重新运行：

```bash
npm run verify
```

If a package is already published with a bad release, prefer a follow-up patch release over unpublish unless the package violates npm policy or contains sensitive data.

如果错误版本已经发布，优先发一个后续 patch 修复版本；除非违反 npm 政策或包含敏感数据，否则不要轻易 unpublish。
