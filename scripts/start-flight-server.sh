#!/bin/bash
#===----------------------------------------------------------------------===//
#                         PostHog DuckDB Extension
#
# Starts the Flight SQL test server independently of test execution.
# Following DuckLake pattern: external service lifecycle is separate from tests.
#
# Usage:
#   ./scripts/start-flight-server.sh              # Foreground (for local dev)
#   ./scripts/start-flight-server.sh --background # Background (for CI)
#   ./scripts/start-flight-server.sh --stop       # Stop background server
#===----------------------------------------------------------------------===//

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_ROOT}/test/integration/.venv"
PID_FILE="${PROJECT_ROOT}/test/integration/.flight_server.pid"

FLIGHT_HOST="${FLIGHT_HOST:-127.0.0.1}"
FLIGHT_PORT="${FLIGHT_PORT:-8815}"

log_info() {
    echo "[INFO] $1"
}

log_error() {
    echo "[ERROR] $1" >&2
}

setup_venv() {
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 not found. Please install Python 3."
        exit 1
    fi

    if [ ! -d "$VENV_DIR" ]; then
        log_info "Creating Python virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi

    source "$VENV_DIR/bin/activate"

    if ! python3 -c "import pyarrow.flight" 2>/dev/null || ! python3 -c "import duckdb" 2>/dev/null; then
        log_info "Installing Python dependencies..."
        pip install --quiet --upgrade pip
        pip install --quiet -r "${PROJECT_ROOT}/test/integration/requirements.txt"
    fi
}

stop_server() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping Flight server (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            rm -f "$PID_FILE"
        else
            log_info "Flight server not running (stale PID file)"
            rm -f "$PID_FILE"
        fi
    else
        log_info "No PID file found"
    fi
}

start_server() {
    local background=false
    if [ "$1" = "--background" ]; then
        background=true
    fi

    setup_venv

    log_info "Starting Flight SQL server on ${FLIGHT_HOST}:${FLIGHT_PORT}..."

    if [ "$background" = true ]; then
        python3 "${PROJECT_ROOT}/test/integration/flight_server.py" \
            --host "$FLIGHT_HOST" \
            --port "$FLIGHT_PORT" &
        echo $! > "$PID_FILE"
        log_info "Flight server started in background (PID: $(cat $PID_FILE))"

        # Wait for server to be ready
        local attempt=0
        while [ $attempt -lt 10 ]; do
            if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('$FLIGHT_HOST', $FLIGHT_PORT)); s.close()" 2>/dev/null; then
                log_info "Flight server is ready!"
                return 0
            fi
            sleep 1
            attempt=$((attempt + 1))
        done
        log_error "Flight server failed to start"
        exit 1
    else
        python3 "${PROJECT_ROOT}/test/integration/flight_server.py" \
            --host "$FLIGHT_HOST" \
            --port "$FLIGHT_PORT"
    fi
}

case "${1:-}" in
    --stop)
        stop_server
        ;;
    --background)
        start_server --background
        ;;
    *)
        start_server
        ;;
esac
