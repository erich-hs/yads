FROM ghcr.io/astral-sh/uv:0.5-python3.10-bookworm-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_CACHE_DIR=/root/.cache/uv

WORKDIR /workspace

COPY uv.lock pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --group sql

COPY README.md ./
COPY src/ ./src/
COPY tests/fixtures/ ./tests/fixtures/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --group sql

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install duckdb

RUN uv cache prune --ci

FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/src /workspace/src
COPY --from=builder /workspace/tests/fixtures /workspace/tests/fixtures

ENV VIRTUAL_ENV=/workspace/.venv
ENV PATH="/workspace/.venv/bin:$PATH"

CMD ["python3", "/workspace/ci/integration-tests/scripts/test-duckdb.py"]
