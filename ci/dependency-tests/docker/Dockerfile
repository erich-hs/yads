FROM ghcr.io/astral-sh/uv:0.5-python3.10-bookworm-slim

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_CACHE_DIR=/root/.cache/uv

WORKDIR /workspace

COPY uv.lock pyproject.toml ./

COPY README.md ./
COPY src/ ./src/
COPY tests/ ./tests/
COPY ci/dependency-tests/scripts/test-deps.sh ./ci/dependency-tests/scripts/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --group dev --no-install-project

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --group dev

RUN uv cache prune --ci

ENTRYPOINT ["/workspace/ci/dependency-tests/scripts/test-deps.sh"]
