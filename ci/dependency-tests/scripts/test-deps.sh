#!/usr/bin/env bash
set -euo pipefail

# Usage: ./ci/dependency-tests/scripts/test-deps.sh <dependency> <version>
# Example: ./ci/dependency-tests/scripts/test-deps.sh pyspark 3.5.3

DEPENDENCY="${1:-}"
VERSION="${2:-}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

error() {
    echo -e "${RED}Error: $*${NC}" >&2
    exit 1
}

info() {
    echo -e "${GREEN}▶ $*${NC}"
}

warn() {
    echo -e "${YELLOW}⚠ $*${NC}"
}

if [ -z "$DEPENDENCY" ] || [ -z "$VERSION" ]; then
    error "Usage: $0 <dependency> <version>"
fi

get_test_path() {
    case "$1" in
        pyspark)
            echo "tests/converters/test_pyspark_converter.py tests/loaders/test_pyspark_loader.py"
            ;;
        pyarrow)
            echo "tests/converters/test_pyarrow_converter.py tests/loaders/test_pyarrow_loader.py"
            ;;
        pydantic)
            echo "tests/converters/test_pydantic_converter.py"
            ;;
        polars)
            echo "tests/converters/test_polars_converter.py tests/loaders/test_polars_loader.py"
            ;;
        *)
            error "Unknown dependency: $1"
            ;;
    esac
}

TEST_PATH=$(get_test_path "$DEPENDENCY")

info "Testing ${DEPENDENCY} ${VERSION}"
echo "==========================================="

info "Adding ${DEPENDENCY} ${VERSION} to project..."
uv add "${DEPENDENCY}==${VERSION}"

info "Syncing environment with dev dependencies and ${DEPENDENCY}..."
uv sync --group dev --group "${DEPENDENCY}"

info "Running tests..."
# shellcheck disable=SC2086
uv run pytest $TEST_PATH -v

info "Pruning uv cache..."
uv cache prune --ci

echo "==========================================="
info "Tests passed for ${DEPENDENCY} ${VERSION}"
