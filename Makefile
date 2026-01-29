# Quick reference for common development tasks. Run 'make help' for details.

# Default target: show help
.DEFAULT_GOAL := help

# Setup Commands
.PHONY: install
install:
	uv sync --frozen --group dev
	uv run --group dev pre-commit install --install-hooks

.PHONY: install-all
install-all:
	uv sync --frozen --all-groups
	uv run --group dev pre-commit install --install-hooks

.PHONY: sync
sync:
	uv sync --group dev

# Development Commands
.PHONY: lint
lint:
	uvx ruff check src/ tests/ ci/

.PHONY: format
format:
	uvx ruff format src/ tests/ ci/

.PHONY: pre-commit
pre-commit:
	uv run --group dev pre-commit run --all-files

# Testing Commands
.PHONY: test
test:
	uv run --all-groups pytest

.PHONY: test-cov
test-cov:
	uv run --all-groups pytest --cov=src --cov-branch --cov-report=html

# Dependency Testing Commands
.PHONY: test-dependency
test-dependency:  # Test compatibility across different versions of a specific optional dependency.
	@if [ -z "$(DEP)" ] || [ -z "$(VER)" ]; then \
		echo "Error: DEP and VER must be specified"; \
		echo "Usage: make test-dependency DEP=pyspark VER=3.5.3"; \
		exit 1; \
	fi
	@docker build -t yads-test:latest -f ci/dependency-tests/docker/Dockerfile .
	@docker run --rm yads-test:latest $(DEP) $(VER)

.PHONY: test-dependency-all
test-dependency-all:  # Test compatibility across all versions of a specific optional dependency.
	@if [ -z "$(DEP)" ]; then \
		echo "Error: DEP must be specified"; \
		echo "Usage: make test-dependency-all DEP=pyspark"; \
		exit 1; \
	fi
	@docker build -t yads-test:latest -f ci/dependency-tests/docker/Dockerfile .
	@echo "Testing all $(DEP) versions..."
	@jq -r '.$(DEP)[]' ci/dependency-tests/versions.json | while read -r ver; do \
		echo ""; \
		docker run --rm yads-test:latest $(DEP) "$$ver" || exit 1; \
	done

# Integration Testing Commands
.PHONY: test-integration
test-integration:  # Run integration tests for a specific SQL dialect.
	@if [ -z "$(DIALECT)" ]; then \
		echo "Error: DIALECT must be specified"; \
		echo "Usage: make test-integration DIALECT=spark"; \
		echo "Available dialects: spark, duckdb"; \
		echo "Use 'make test-integration-all' to test all dialects"; \
		exit 1; \
	fi
	cd ci/integration-tests && ./scripts/run-integration.sh $(DIALECT)

.PHONY: test-integration-all
test-integration-all:  # Run integration tests for all SQL dialects.
	cd ci/integration-tests && ./scripts/run-integration.sh

# Build Commands
.PHONY: build
build:
	uv build
	@ls -lh dist/

# Cleanup Commands
.PHONY: clean
clean:  # Clean test artifacts and caches.
	rm -rf .coverage htmlcov .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -name "*.py[co]" -delete

.PHONY: clean-all
clean-all:  # Clean all build artifacts and caches.
	$(MAKE) clean
	rm -rf .mypy_cache .ruff_cache dist

# Documentation Commands
.PHONY: docs-build
docs-build:
	uv run --group docs zensical build

.PHONY: docs-serve
docs-serve:
	uv run --group docs zensical serve

.PHONY: sync-examples
sync-examples:
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE must be specified"; \
		echo "Usage: make sync-examples FILE=docs/converters/pyarrow.md"; \
		exit 1; \
	fi
	uv run --all-groups python -m docs.src.scripts.sync_examples "$(FILE)"

.PHONY: sync-examples-all
sync-examples-all:
	uv run --all-groups python -m docs.src.scripts.sync_examples --all

# Help
.PHONY: help
help:
	@echo "yads Makefile Commands"
	@echo "======================"
	@echo ""
	@echo "Setup Commands:"
	@echo "  make install          Install core and development dependencies and setup pre-commit hooks"
	@echo "  make install-all      Install all dependencies and setup hooks"
	@echo "  make sync             Update dependencies to match lockfile"
	@echo ""
	@echo "Development Commands:"
	@echo "  make lint             Check code formatting and linting"
	@echo "  make format           Auto-format code"
	@echo "  make pre-commit       Run all pre-commit hooks"
	@echo ""
	@echo "Testing Commands:"
	@echo "  make test             Run test suite"
	@echo "  make test-cov         Run tests with coverage report"
	@echo ""
	@echo "Dependency Testing Commands:"
	@echo "  make test-dependency DEP=<name> VER=<version>"
	@echo "                        Test specific dependency version (e.g., DEP=pyspark VER=3.5.3)"
	@echo "  make test-dependency-all DEP=<name>"
	@echo "                        Test all versions of a dependency"
	@echo ""
	@echo "Integration Testing Commands:"
	@echo "  make test-integration DIALECT=<name>"
	@echo "                        Run integration tests for specific dialect (spark, duckdb)"
	@echo "  make test-integration-all"
	@echo "                        Run integration tests for all dialects"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build            Build package distributions"
	@echo ""
	@echo "Cleanup Commands:"
	@echo "  make clean            Remove test artifacts and caches"
	@echo "  make clean-all        Remove all build artifacts and caches"
	@echo ""
	@echo "Documentation Commands:"
	@echo "  make docs-build       Build the documentation"
	@echo "  make docs-serve       Serve the documentation"
	@echo "  make sync-examples FILE=<markdown>"
	@echo "                        Sync code blocks for a single docs file"
	@echo "  make sync-examples-all"
	@echo "                        Sync code blocks across all docs"
