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
    uv pip install psycopg2-binary

RUN uv cache prune --ci

FROM python:3.10-slim

# Install PostgreSQL
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        postgresql \
        postgresql-contrib \
        sudo \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/src /workspace/src
COPY --from=builder /workspace/tests/fixtures /workspace/tests/fixtures

# Copy entrypoint script
COPY ci/integration-tests/docker/postgres/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV VIRTUAL_ENV=/workspace/.venv
ENV PATH="/workspace/.venv/bin:$PATH"

# PostgreSQL configuration
ENV POSTGRES_USER=yads
ENV POSTGRES_PASSWORD=yads
ENV POSTGRES_DB=yads_test

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python3", "/workspace/ci/integration-tests/scripts/test-postgres.py"]
