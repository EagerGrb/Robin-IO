# Release Checklist

This project uses npm workspaces and Changesets. The root package is private; publishable packages live under `packages/*`.

## Before Release

Run the full local quality gate:

```bash
npm run verify
npm run bench
```

Check package metadata and exports:

```bash
npm run package:check
npm run api:check
npm run smoke:node-dist
npm run smoke:browser-worker
```

Check Changesets configuration:

```bash
npm run changeset:check-config
```

Review release readiness policy:

```text
docs/release/deployment-and-growth-plan.md
docs/release/readiness.md
docs/release/rehearsal.md
docs/release/ci-matrix.md
docs/release/candidate-record.md
```

Run the real browser worker benchmark when Chrome or Edge is available:

```bash
npm run bench:browser-worker
```

## Create Release Notes

Create a changeset for user-visible package changes:

```bash
npm run changeset
```

Review pending release status in a git branch with `main` available:

```bash
npm run changeset:status
```

Create a release candidate record before approval:

```text
docs/release/candidate-record.md
```

## Version Packages

Apply version bumps and changelog updates:

```bash
npm run changeset:version
npm run verify
```

## Publish

Publishing is intentionally guarded. Before publishing, confirm:

- npm account and organization/scope access;
- package names;
- provenance requirements;
- GitHub environment approval rules;
- Trusted Publishing or `NPM_TOKEN` fallback;
- `main` branch protection and release workflow access.

Use the manual release workflow for the first public releases. Do not publish from a local machine unless the same release gate has passed in CI.

## Rollback

If a release fails before publishing, revert the version/changelog commit and rerun:

```bash
npm run verify
```

If a package is already published with a bad release, prefer a follow-up patch release over unpublish unless the package violates npm policy or contains sensitive data.
