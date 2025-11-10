# Release Guide

Instructions for releasing new versions of yads to PyPI and GitHub.

## Overview

`yads` uses a manual release process that gives maintainers full control over when and what gets released. The workflow is designed to be deterministic, transparent, and easy to verify locally before publishing.

**Key components:**

- **Release Drafter** - Automatically drafts release notes from merged PRs
- **uv version** - Built-in version management
- **Manual workflow** - GitHub Actions workflow with dry-run support
- **Trusted publishing** - Secure PyPI publishing via OIDC

## Semantic Versioning

yads follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (X.0.0) - Breaking changes
- **MINOR** (0.X.0) - New features, backward compatible
- **PATCH** (0.0.X) - Bug fixes and performance improvements

**Pre-1.0 strategy:** Currently in 0.x.y where minor bumps indicate features and patch bumps indicate fixes. Breaking changes are acceptable until 1.0.0.

**Release timing:** On-demand when changes accumulate, important fixes are ready, or new features are complete.

## Version Management with uv

Check current version:

```bash
uv version
# yads 0.0.1

uv version --short
# 0.0.1
```

Set specific version:

```bash
uv version 0.0.2
# yads 0.0.1 => 0.0.2
```

Bump version components:

```bash
uv version --bump patch  # 0.0.1 -> 0.0.2
uv version --bump minor  # 0.0.1 -> 0.1.0
uv version --bump major  # 0.0.1 -> 1.0.0
```

Preview changes without modifying files:

```bash
uv version 0.0.2 --dry-run
# yads 0.0.1 => 0.0.2
```

Verify the build works after version changes:

```bash
uv build
```

## Release Process

### 1. Prepare for release

Ensure all intended PRs are merged to `main`:

```bash
git checkout main
git pull origin main
```

### 2. Review draft release

Navigate to the [releases page](https://github.com/erich-hs/yads/releases) and locate the draft release created by Release Drafter. Review the auto-generated changelog and edit as needed. Note the suggested version number.

### 3. Determine version

Choose the appropriate version based on changes:

- New features → minor bump (0.x.0)
- Bug fixes only → patch bump (0.0.x)
- Breaking changes → major bump (for 1.0.0+)

### 4. Update version

```bash
uv version 0.0.2

# Or use bump commands
uv version --bump patch
```

### 5. Verify build

```bash
uv build
ls -lh dist/
```

Fix any build issues before proceeding.

### 6. Commit and push

```bash
git commit -am "release: bump version to 0.0.2"
git push origin main
```

### 7. Test release (optional)

Go to the [release workflow](https://github.com/erich-hs/yads/actions/workflows/release-python.yml), click "Run workflow", select `main` branch, enable "Dry run", and trigger.

Dry-run will build and validate but not publish. Review the logs before proceeding.

### 8. Execute release

Trigger the workflow again with dry-run disabled. The workflow will:

1. Build the package
2. Publish to PyPI via trusted publishing
3. Create and push git tag
4. Publish GitHub release

### 9. Verify publication

Check PyPI installation:

```bash
pip install --upgrade yads
python -c "import yads; print(yads.__version__)"
```

Verify the [GitHub release](https://github.com/erich-hs/yads/releases) is published and the tag exists:

```bash
git fetch --tags
git tag | grep v0.0.2
```

## PR Title Conventions

Release Drafter categorizes changes based on PR titles. Use [conventional commit](https://www.conventionalcommits.org/) format following the [Angular convention](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#type).

Common prefixes: `feat:`, `fix:`, `perf:`, `docs:`, `test:`, `refactor:`, `chore:`, `ci:`, `build:`

Optional scope in parentheses: `feat(converters):`, `fix(loaders):`

Examples:

```
feat: add support for BigQuery SQL dialect
fix: handle null values in nested struct fields
perf(converters): optimize PyArrow schema conversion
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for complete guidelines.

## Dry-Run Testing

Use dry-run to test the workflow without publishing. Useful for first releases, workflow changes, or verification.

Dry-run will:
- Build package and validate version
- Upload artifacts for review
- Skip PyPI publishing, tag creation, and GitHub release

To use dry-run, follow steps 1-6, then trigger the workflow with the "Dry run" option enabled. Review logs and artifacts before running the real release.

## Troubleshooting

**Tag already exists**

A git tag for this version exists. Delete it and retry:

```bash
git fetch --tags
git tag -d v0.0.2
git push origin :refs/tags/v0.0.2
```

**PyPI publish failed**

Version already exists on PyPI (cannot be overwritten) or invalid token.

For duplicate version, bump to a new version:

```bash
uv version --bump patch
git commit -am "release: bump version to 0.0.3"
git push origin main
```

For invalid token, regenerate at https://pypi.org/manage/account/token/ and update the `PYPI_TOKEN` secret in repository Settings → Secrets and variables → Actions.

**No draft release found**

Release Drafter hasn't created a draft yet. The workflow will create a basic release automatically. Edit the release notes afterward if needed.

**Version mismatch**

Version in `pyproject.toml` doesn't match expectations. Verify you're on `main` and committed the version change:

```bash
uv version
git branch --show-current
git checkout main
git pull origin main
uv version 0.0.2
git commit -am "release: bump version to 0.0.2"
git push origin main
```

**Build fails locally**

Run tests and linting to identify issues:

```bash
make test
make lint
uv build
```

**Cancel in-progress release**

Navigate to [GitHub Actions](https://github.com/erich-hs/yads/actions), find the running workflow, and click "Cancel workflow". If PyPI publish succeeded, you cannot unpublish—release a new version instead. Delete created tags if needed.

## Additional Resources

View all releases: https://github.com/erich-hs/yads/releases

Compare versions:

```bash
git log v0.0.1..v0.0.2 --oneline
git log $(git describe --tags --abbrev=0)..HEAD --oneline
```

For release process issues, check GitHub Actions logs and ensure CI passes on `main`. Open an issue for unexpected problems.
