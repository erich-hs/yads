# CI Infrastructure

This directory contains all CI/CD infrastructure for testing yads across multiple dependency versions.

## Structure

```
ci/
├── docker/
│   └── Dockerfile.deps      # Docker image for dependency testing
├── scripts/
│   └── test-deps.sh         # Test script (runs in Docker and CI)
├── deps-versions.json       # Version matrix for all dependencies
└── README.md                # This file
```

## Quick Start

```bash
# Test specific version
make test-deps DEP=pyspark VER=3.5.3

# Test all versions of a dependency
make test-deps-all DEP=pyspark
```

## Files

### `docker/Dockerfile.deps`
Docker image for isolated dependency version testing. Includes:
- Python 3.10 base
- uv package manager
- Project files (via COPY, respects .dockerignore)

### `scripts/test-deps.sh`
Main test script that:
1. Installs dev dependencies (frozen)
2. Installs project in editable mode
3. Installs specific dependency version
4. Runs relevant tests

Works both in Docker and directly in CI environments.

### `deps-versions.json`
Version matrix defining which versions to test for each optional dependency.

## Usage

### Local Testing

```bash
# Via Makefile (recommended - uses Docker)
make test-deps DEP=pyspark VER=3.5.3
make test-deps-all DEP=pyspark

# Direct script (modifies local environment - not recommended)
./ci/scripts/test-deps.sh pyspark 3.5.3
```

### CI Testing

GitHub Actions workflow (`.github/workflows/test-deps.yml`) automatically:
- Loads version matrix from `deps-versions.json`
- Creates test jobs for all dependency/version combinations
- Runs tests in Docker using the same script

### Manual Workflow Dispatch

Trigger tests manually from GitHub:
1. Go to Actions → Dependency Tests
2. Click "Run workflow"
3. Select dependency and optionally version

## Adding New Versions

Edit `deps-versions.json`:

```json
{
  "pyspark": ["3.1.3", "3.2.4", "new-version-here"]
}
```

CI will automatically test the new version on the next push.

## Design Goals

- **Simple**: Single script, single config file
- **Isolated**: Docker prevents local pollution
- **Extensible**: Adding dependencies/versions is trivial
- **Reproducible**: Same behavior locally and in CI

