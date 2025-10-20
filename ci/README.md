# CI Infrastructure

Testing infrastructure for yads across dependency versions and database environments.

## Quick Start

```bash
# Dependency version tests
make test-deps DEP=pyspark VER=3.5.3
make test-deps-all DEP=pyspark

# Integration tests
make integration-test-spark
make integration-test-duckdb
make integration-test-all
```

## Dependency Tests

Tests compatibility across multiple versions of optional dependencies (pyspark, pyarrow, pydantic). Each dependency is tested in isolation to ensure true compatibility.

### Files

- `dependency-tests/versions.json` - Version matrix
- `dependency-tests/docker/Dockerfile` - Test environment
- `dependency-tests/scripts/test-deps.sh` - Test runner

### Usage

```bash
# Local testing (via Makefile)
make test-deps DEP=pyspark VER=3.5.3

# Direct script
cd ci/dependency-tests
./scripts/test-deps.sh pyspark 3.5.3
```

### Adding Versions

Edit `dependency-tests/versions.json`:

```json
{
  "pyspark": ["3.1.1", "4.0.1", "new-version"]
}
```

## Integration Tests

End-to-end tests that validate SQL converters by executing generated DDL in actual database environments.

### Supported Dialects

- **Spark**: Apache Spark 3.5+ with Iceberg extension
- **DuckDB**: DuckDB with Python API

### Files

- `integration-tests/config.json` - Test configuration
- `integration-tests/docker/<dialect>/Dockerfile` - Database environments
- `integration-tests/scripts/test-<dialect>.py` - Test scripts
- `integration-tests/scripts/run-integration.sh` - Orchestration

### Usage

```bash
# Local testing
make integration-test-spark
make integration-test-duckdb

# Direct script
cd ci/integration-tests
./scripts/run-integration.sh spark
```

### Adding Dialects

1. Update `integration-tests/config.json` with new dialect and Docker image name
2. Create Dockerfile in `integration-tests/docker/<dialect>/`
3. Create test script in `integration-tests/scripts/test-<dialect>.py`
4. Add Make target (optional)

## Troubleshooting

### Docker Build Failures
- Check `uv.lock` is up to date: `uv lock`
- Build from project root: `docker build -f ci/<path>/Dockerfile .`
- Clear build cache: `docker builder prune`

### Test Failures
- Use verbose output: `pytest -v`
- Test locally first: `make integration-test-<dialect>`
- Check target logs in containers

### Environment Issues
- Always use Make targets (they use Docker)
- Never run scripts directly
- Clean up: `docker system prune`