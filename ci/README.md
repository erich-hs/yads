# CI Infrastructure

Testing infrastructure for yads across dependency versions and database environments.

## Quick Start

```bash
# Dependency version tests
make test-deps DEP=pyspark VER=3.5.3
make test-deps-all DEP=pyspark

# Integration tests
make integration-test DIALECT=spark
make integration-test DIALECT=duckdb
make integration-test-all
```

## Dependency Tests

Tests compatibility across multiple versions of optional dependencies (pyspark, pyarrow, pydantic, polars). Each dependency is tested in isolation to ensure true compatibility.

The version matrix is defined in [`dependency-tests/versions.json`](dependency-tests/versions.json).

### Usage

```bash
# Local testing (via Makefile)
make test-deps DEP=pyspark VER=3.5.3

# Direct script
cd ci/dependency-tests
./scripts/test-deps.sh pyspark 3.5.3
```

### Adding Versions

Edit [`dependency-tests/versions.json`](dependency-tests/versions.json) and add the version to the appropriate dependency array. The CI workflow automatically picks up new versions.

## Integration Tests

End-to-end tests that validate SQL converters by executing generated DDL in actual database environments.

Supported dialects are defined in [`integration-tests/config.json`](integration-tests/config.json).

### Usage

```bash
# Local testing
make integration-test DIALECT=spark
make integration-test DIALECT=duckdb

# Direct script
cd ci/integration-tests
./scripts/run-integration.sh spark
```

### Adding Dialects

1. Update [`integration-tests/config.json`](integration-tests/config.json) with new dialect and Docker image name
2. Create Dockerfile in `integration-tests/docker/<dialect>/`
3. Create test script in `integration-tests/scripts/test-<dialect>.py`
4. Add Make target

## Troubleshooting

### Docker Build Failures
- Check `uv.lock` is up to date: `uv lock`
- Build from project root: `docker build -f ci/<path>/Dockerfile .`
- Clear build cache: `docker builder prune`

### Test Failures
- Use verbose output: `pytest -v`
- Test locally first: `make integration-test DIALECT=<dialect>`
- Check target logs in containers

### Environment Issues
- Always use Make targets (they use Docker)
- Never run scripts directly
- Clean up: `docker system prune`