#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run-integration.sh [dialect]
# Examples: ./run-integration.sh spark | ./run-integration.sh duckdb | ./run-integration.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEGRATION_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(cd "$INTEGRATION_DIR/../.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

error() {
    echo -e "${RED}Error: $*${NC}" >&2
    exit 1
}

info() {
    echo -e "${BLUE}ℹ $*${NC}"
}

success() {
    echo -e "${GREEN}▶ $*${NC}"
}

warn() {
    echo -e "${YELLOW}⚠ $*${NC}"
}

REQUESTED_DIALECT="${1:-all}"

CONFIG_FILE="$INTEGRATION_DIR/config.json"
info "Loading configuration from $CONFIG_FILE"

if [ ! -f "$CONFIG_FILE" ]; then
    error "Configuration file not found: $CONFIG_FILE"
fi

AVAILABLE_DIALECTS=$(jq -r '.dialects | keys[]' "$CONFIG_FILE")

if [ "$REQUESTED_DIALECT" = "all" ]; then
    DIALECTS_TO_TEST=$AVAILABLE_DIALECTS
else
    if ! echo "$AVAILABLE_DIALECTS" | grep -q "^${REQUESTED_DIALECT}$"; then
        error "Unknown dialect: $REQUESTED_DIALECT. Available: $(echo $AVAILABLE_DIALECTS | tr '\n' ' ')"
    fi
    DIALECTS_TO_TEST=$REQUESTED_DIALECT
fi

success "Starting integration tests..."
echo "==========================================="

OVERALL_STATUS=0

for dialect in $DIALECTS_TO_TEST; do
    info "Processing dialect: $dialect"
    
    if command -v jq &> /dev/null; then
        IMAGE_NAME=$(jq -r ".dialects.${dialect}.docker_image" "$CONFIG_FILE")
    else
        IMAGE_NAME="yads-integration-${dialect}"
    fi
    
    success "Building Docker image for $dialect..."
    DOCKERFILE="$INTEGRATION_DIR/docker/$dialect/Dockerfile"
    
    if [ ! -f "$DOCKERFILE" ]; then
        error "Dockerfile not found: $DOCKERFILE"
    fi
    
    if ! docker build -t "$IMAGE_NAME" -f "$DOCKERFILE" "$PROJECT_ROOT"; then
        error "Failed to build Docker image for $dialect"
    fi
    
    success "Docker image built: $IMAGE_NAME"
    
    TEST_SCRIPT="$SCRIPT_DIR/test-${dialect}.py"
    if [ ! -f "$TEST_SCRIPT" ]; then
        error "Test script not found: $TEST_SCRIPT"
    fi
    
    success "Running integration tests for $dialect..."
    echo "-------------------------------------------"
    
    if docker run --rm \
        -v "$SCRIPT_DIR:/workspace/ci/integration-tests/scripts:ro" \
        "$IMAGE_NAME"; then
        success "$dialect integration tests PASSED"
    else
        warn "$dialect integration tests FAILED"
        OVERALL_STATUS=1
    fi
    
    echo "==========================================="
done
echo ""
if [ $OVERALL_STATUS -eq 0 ]; then
    success "All integration tests PASSED ✓"
else
    error "Some integration tests FAILED ✗"
fi

exit $OVERALL_STATUS

