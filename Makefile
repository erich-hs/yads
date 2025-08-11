lint:
	uvx ruff check .

format:
	uvx ruff format . 

test:
	uv run --group dev pytest

test-cov:
	uv run --group dev pytest --cov=src --cov-report=html

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