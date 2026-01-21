#!/bin/bash
#===----------------------------------------------------------------------===//
#                         PostHog DuckDB Extension
#
# Integration test runner script
# Starts a Python Flight SQL server and runs integration tests
#===----------------------------------------------------------------------===//

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build/release"
VENV_DIR="${SCRIPT_DIR}/.venv"

# Configuration
FLIGHT_PORT="${FLIGHT_PORT:-8815}"
FLIGHT_HOST="${FLIGHT_HOST:-127.0.0.1}"
SERVER_STARTUP_TIMEOUT=10
SERVER_PID=""
DUCKDB_BIN="${DUCKDB_BIN:-${BUILD_DIR}/duckdb}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        log_info "Stopping Flight SQL server (PID: $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

setup_venv() {
    # Check for Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 not found. Please install Python 3."
        exit 1
    fi

    # Create virtual environment if it doesn't exist
    if [ ! -d "$VENV_DIR" ]; then
        log_info "Creating Python virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi

    # Activate virtual environment
    source "$VENV_DIR/bin/activate"

    # Install dependencies if needed
    if ! python3 -c "import pyarrow.flight" 2>/dev/null || ! python3 -c "import duckdb" 2>/dev/null; then
        log_info "Installing Python dependencies..."
        pip install --quiet --upgrade pip
        pip install --quiet -r "${SCRIPT_DIR}/requirements.txt"
    fi

    log_info "Python virtual environment ready"
}

check_dependencies() {
    # Check for DuckDB CLI
    if [ ! -x "$DUCKDB_BIN" ]; then
        log_error "DuckDB CLI not found at: $DUCKDB_BIN"
        log_error "Build the project first with 'make release' or set DUCKDB_BIN"
        exit 1
    fi

    # Check for extension build
    if [ ! -f "${BUILD_DIR}/extension/posthog/posthog.duckdb_extension" ]; then
        log_error "Extension not found. Please build the extension first with 'make release'."
        exit 1
    fi

    log_info "Dependencies check passed"
}

start_flight_server() {
    log_info "Starting Python Flight SQL server on ${FLIGHT_HOST}:${FLIGHT_PORT}..."

    # Start the Python Flight SQL server in the background
    python3 "${SCRIPT_DIR}/flight_server.py" --host "${FLIGHT_HOST}" --port "${FLIGHT_PORT}" &
    SERVER_PID=$!

    log_info "Flight server started with PID: $SERVER_PID"

    # Wait for server to be ready
    log_info "Waiting for Flight server to be ready..."
    local attempt=0
    while [ $attempt -lt $SERVER_STARTUP_TIMEOUT ]; do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            log_error "Flight server exited unexpectedly"
            exit 1
        fi

        # Try to connect to the port using Python
        if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('$FLIGHT_HOST', $FLIGHT_PORT)); s.close()" 2>/dev/null; then
            log_info "Flight server is ready!"
            return 0
        fi

        sleep 1
        attempt=$((attempt + 1))
    done

    log_error "Flight server failed to start within ${SERVER_STARTUP_TIMEOUT} seconds"
    exit 1
}

run_sql_tests() {
    log_info "Running SQL integration tests..."

    local test_count=0
    local pass_count=0
    local fail_count=0
    local ext_path="${BUILD_DIR}/extension/posthog/posthog.duckdb_extension"

    # Test 1: Basic SELECT 1
    log_info "Test 1: Basic SELECT 1..."
    if $DUCKDB_BIN -cmd "
LOAD '${ext_path}';
ATTACH 'hog:test?token=demo&endpoint=grpc://${FLIGHT_HOST}:${FLIGHT_PORT}' AS remote;
SELECT 1;
" 2>&1 | grep -q "1"; then
        log_info "  PASSED: Basic SELECT 1"
        pass_count=$((pass_count + 1))
    else
        log_error "  FAILED: Basic SELECT 1"
        fail_count=$((fail_count + 1))
    fi
    test_count=$((test_count + 1))

    # Test 2: Query test_data table
    log_info "Test 2: Query test_data table..."
    if $DUCKDB_BIN -cmd "
LOAD '${ext_path}';
ATTACH 'hog:test?token=demo&endpoint=grpc://${FLIGHT_HOST}:${FLIGHT_PORT}' AS remote;
SELECT * FROM remote.main.test_data;
" 2>&1 | grep -q "1"; then
        log_info "  PASSED: Query test_data table"
        pass_count=$((pass_count + 1))
    else
        log_error "  FAILED: Query test_data table"
        fail_count=$((fail_count + 1))
    fi
    test_count=$((test_count + 1))

    # Test 3: Query numbers table
    log_info "Test 3: Query numbers table..."
    if $DUCKDB_BIN -cmd "
LOAD '${ext_path}';
ATTACH 'hog:test?token=demo&endpoint=grpc://${FLIGHT_HOST}:${FLIGHT_PORT}' AS remote;
SELECT COUNT(*) FROM remote.main.numbers;
" 2>&1 | grep -q "10"; then
        log_info "  PASSED: Query numbers table"
        pass_count=$((pass_count + 1))
    else
        log_error "  FAILED: Query numbers table"
        fail_count=$((fail_count + 1))
    fi
    test_count=$((test_count + 1))

    # Test 4: Schema listing
    log_info "Test 4: Schema listing..."
    if $DUCKDB_BIN -cmd "
LOAD '${ext_path}';
ATTACH 'hog:test?token=demo&endpoint=grpc://${FLIGHT_HOST}:${FLIGHT_PORT}' AS remote;
SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'remote';
" 2>&1 | grep -qi "main"; then
        log_info "  PASSED: Schema listing"
        pass_count=$((pass_count + 1))
    else
        log_error "  FAILED: Schema listing"
        fail_count=$((fail_count + 1))
    fi
    test_count=$((test_count + 1))

    # Test 5: Table listing
    log_info "Test 5: Table listing..."
    if $DUCKDB_BIN -cmd "
LOAD '${ext_path}';
ATTACH 'hog:test?token=demo&endpoint=grpc://${FLIGHT_HOST}:${FLIGHT_PORT}' AS remote;
SHOW TABLES FROM remote.main;
" 2>&1 | grep -qi "test_data\|numbers"; then
        log_info "  PASSED: Table listing"
        pass_count=$((pass_count + 1))
    else
        log_error "  FAILED: Table listing"
        fail_count=$((fail_count + 1))
    fi
    test_count=$((test_count + 1))

    # Test 6: Column projection
    log_info "Test 6: Column projection..."
    if $DUCKDB_BIN -cmd "
LOAD '${ext_path}';
ATTACH 'hog:test?token=demo&endpoint=grpc://${FLIGHT_HOST}:${FLIGHT_PORT}' AS remote;
SELECT name FROM remote.main.numbers WHERE id = 5;
" 2>&1 | grep -q "item_5"; then
        log_info "  PASSED: Column projection"
        pass_count=$((pass_count + 1))
    else
        log_error "  FAILED: Column projection"
        fail_count=$((fail_count + 1))
    fi
    test_count=$((test_count + 1))

    # Summary
    echo ""
    log_info "========================================="
    log_info "Integration Test Summary"
    log_info "========================================="
    log_info "Total:  $test_count"
    log_info "Passed: $pass_count"
    if [ $fail_count -gt 0 ]; then
        log_error "Failed: $fail_count"
    else
        log_info "Failed: $fail_count"
    fi
    log_info "========================================="

    if [ $fail_count -gt 0 ]; then
        exit 1
    fi
}

main() {
    log_info "PostHog DuckDB Extension - Integration Tests"
    log_info "============================================="

    setup_venv
    check_dependencies
    start_flight_server
    run_sql_tests

    log_info "All integration tests passed!"
}

main "$@"
