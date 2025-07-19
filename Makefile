lint:
	uvx ruff check .

format:
	uvx ruff format . 

test:
	uv run pytest

count-lines:
	find . -name "*.py" -not -path "./.venv/*" -not -path "./.git/*" -not -path "./__pycache__/*" -not -path "./.pytest_cache/*" -not -path "./.mypy_cache/*" -not -path "./.ruff_cache/*" -not -path "./dist/*" | xargs wc -l