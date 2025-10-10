#!/usr/bin/env bash
set -euo pipefail

# Dependency version testing script for optional dependencies
# Can run locally or in Docker, used by CI
#
# Usage:
#   ./ci/scripts/test-deps.sh <dependency> <version>
#   ./ci/scripts/test-deps.sh pyspark 3.5.3
#   ./ci/scripts/test-deps.sh pyarrow 21.0.0

DEPENDENCY="${1:-}"
VERSION="${2:-}"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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

# Validate inputs
if [ -z "$DEPENDENCY" ] || [ -z "$VERSION" ]; then
    error "Usage: $0 <dependency> <version>"
fi

# Map dependency to test path
get_test_path() {
    case "$1" in
        pyspark)
            echo "tests/converters/test_pyspark_converter.py"
            ;;
        pyarrow)
            echo "tests/converters/test_pyarrow_converter.py tests/loaders/test_pyarrow_loader.py"
            ;;
        pydantic)
            echo "tests/converters/test_pydantic_converter.py"
            ;;
        *)
            error "Unknown dependency: $1"
            ;;
    esac
}

TEST_PATH=$(get_test_path "$DEPENDENCY")

# Main test execution
info "Testing ${DEPENDENCY} ${VERSION}"
echo "==========================================="

# Sync dev dependencies (frozen to avoid lock file changes)
info "Installing dev dependencies..."
uv sync --frozen --group dev --no-install-project

# Install the project in editable mode
info "Installing project..."
uv pip install -e .

# Install specific version of the dependency
info "Installing ${DEPENDENCY} ${VERSION}..."
uv pip install "${DEPENDENCY}==${VERSION}"

# Run tests
info "Running tests..."
# shellcheck disable=SC2086
uv run --group dev pytest $TEST_PATH -v

echo "==========================================="
info "Tests passed for ${DEPENDENCY} ${VERSION}"

