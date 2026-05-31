# Release Readiness / 发布就绪

This project is still pre-1.0. Release readiness means a package can be published for controlled beta use with explicit compatibility limits.

本项目仍处于 pre-1.0。发布就绪表�?package 可以面向受控 beta 场景发布，并明确兼容性边界�?
## Public API / 公开 API

The stable public entry for each package is its root export, for example:

每个 package 的稳定公开入口是根导出，例如：

```ts
import { pipeline } from "@robbin-io/core"
import { webWorkerTransform } from "@robbin-io/browser"
```

Pre-1.0 compatibility rules:

- Root-exported functions, classes, interfaces, and constants are public unless documented as experimental.
- Package subpaths named `internal` are intentionally unstable and are not application-facing public API.
- Public APIs can still change before 1.0, but breaking changes require a changeset and migration notes.
- Core framework error categories should use `CORE_RUNTIME_ERROR_CODES` when possible.

pre-1.0 兼容规则�?
- 根导出的函数、类、接口和常量默认是公开 API，除非文档标记为 experimental�?- 名为 `internal` �?package 子路径有意保持不稳定，不属于面向应用的公开 API�?- 1.0 前公开 API 仍可能变化，但破坏性变更必须有 changeset 和迁移说明�?- Core 框架级错误分类应尽量使用 `CORE_RUNTIME_ERROR_CODES`�?
`docs/release/api-surface.core.json` records the current `@robbin-io/core` root value exports. `npm run api:check` fails when root value exports drift without an intentional update.

`docs/release/api-surface.core.json` 记录当前 `@robbin-io/core` 根入�?value exports。若根入�?value exports 未经有意更新而漂移，`npm run api:check` 会失败�?
`@robbin-io/core/internal` is a separate implementation entry for framework internals. Its package export is checked so builds do not drift, but its value exports are not part of the stable root API allowlist.

`@robbin-io/core/internal` 是单独的框架内部实现入口。它�?package export 会被检查以避免构建漂移，但它的 value exports 不属于稳定根 API allowlist�?
## Experimental Surface / 实验性表�?
The following surfaces are public for beta feedback but not yet long-term stable:

以下表面�?beta 反馈开放，但尚未承诺长期稳定：

- Detailed runtime event payload shapes beyond `type`, `timestamp`, `stage`, and `metadata`.
- Metric key names for internal stage/channel/batch/sink measurements.
- Browser worker benchmark scripts and fixture packages.
- Internal scheduling behavior used to choose direct, fused, or channel-backed execution paths.

## Internal Surface / 内部表面

Do not depend on files under package `src/` or `dist/` paths directly. Import only through package roots.

不要直接依赖 package �?`src/` �?`dist/` 文件路径；只通过 package 根入口导入�?
Framework-owned packages may import from documented internal subpaths such as `@robbin-io/core/internal` when they need execution internals. Application code should not depend on these internal subpaths.

框架自有 package 如需执行内部能力，可以导入文档化�?internal 子路径，例如 `@robbin-io/core/internal`。应用代码不应依赖这�?internal 子路径�?
The following modules are implementation details even if their types appear in generated declarations:

- scheduler internals
- runner classes
- channel implementation details
- `@robbin-io/core/internal` value exports
- benchmark fixtures
- smoke-test harnesses

## Release Gate / 发布门槛

Before publishing a beta release:

发布 beta 版本前：

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

必需 PR 门槛、发布候选门槛和环境矩阵�?`docs/release/ci-matrix.md`�?
Use `docs/release/candidate-record.md` to record release candidate evidence before approval.

批准前使�?`docs/release/candidate-record.md` 记录 release candidate 证据�?
For environments without Chrome or Edge, document that `smoke:browser-worker` and `bench:browser-worker` require `CHROME_PATH`.

## CI and npm Publish Loop

The repository includes:

- `.github/workflows/ci.yml` for PR/push verification plus Playwright browser matrix.
- `.github/workflows/release.yml` for manual release gates and optional npm publish.
- `npm run release:publish:dry-run` for local or CI publish simulation.
- `npm run release:publish` for actual package publishing from CI with `NPM_TOKEN`.

Real publishing is intentionally guarded:

- use the `main` branch;
- configure repository secret `NPM_TOKEN`;
- configure the `npm` environment if environment approvals are required;
- run the release workflow with `publish: true`;
- do not publish examples, only packages under `packages/*`.

如果环境没有 Chrome �?Edge，需要说�?`smoke:browser-worker` �?`bench:browser-worker` 依赖 `CHROME_PATH`�?