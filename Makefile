lint:
	uvx ruff check .

format:
	uvx ruff format . 

test:
	uv run --group dev pytest

test-cov:
	uv run --group dev pytest --cov=src --cov-report=html

test-deps:
	@if [ -z "$(DEP)" ] || [ -z "$(VER)" ]; then \
		echo "Error: DEP and VER must be specified"; \
		echo "Usage: make test-deps DEP=pyspark VER=3.5.3"; \
		exit 1; \
	fi
	@docker build -t yads-test:latest -f ci/docker/Dockerfile.deps .
	@docker run --rm yads-test:latest $(DEP) $(VER)

test-deps-all:
	@if [ -z "$(DEP)" ]; then \
		echo "Error: DEP must be specified"; \
		echo "Usage: make test-deps-all DEP=pyspark"; \
		exit 1; \
	fi
	@docker build -t yads-test:latest -f ci/docker/Dockerfile.deps .
	@echo "Testing all $(DEP) versions..."
	@jq -r '.$(DEP)[]' ci/deps-versions.json | while read -r ver; do \
		echo ""; \
		docker run --rm yads-test:latest $(DEP) "$$ver" || exit 1; \
	done

count-lines:
	find . -name "*.py" -not -path "./.venv/*" -not -path "./.git/*" -not -path "./__pycache__/*" -not -path "./.pytest_cache/*" -not -path "./.mypy_cache/*" -not -path "./.ruff_cache/*" -not -path "./dist/*" | xargs wc -l

.PHONY: clean clean-all

clean:
	rm -rf .coverage htmlcov .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -name "*.py[co]" -delete

clean-all:
	$(MAKE) clean
	rm -rf .mypy_cache .ruff_cache dist