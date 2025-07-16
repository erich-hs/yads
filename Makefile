lint:
	uvx ruff check .

format:
	uvx ruff format . 

test:
	uv run pytest