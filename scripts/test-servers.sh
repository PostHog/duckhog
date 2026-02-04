#!/bin/bash
#===----------------------------------------------------------------------===//
#                         PostHog DuckDB Extension
#
# Starts/stops integration test servers.
#
# Usage:
#   ./scripts/test-servers.sh start [--background] [--control-plane]
#   ./scripts/test-servers.sh stop
#   ./scripts/test-servers.sh status
#   ./scripts/test-servers.sh env
#
# Notes:
# - `start` without `--control-plane` runs only the Flight SQL server.
# - `--control-plane` starts the mock control plane in addition to Flight SQL.
# - `env` prints export commands; use: eval "$(./scripts/test-servers.sh env)"
#===----------------------------------------------------------------------===//

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

VENV_DIR="${PROJECT_ROOT}/test/integration/.venv"
PID_FLIGHT_FILE="${PROJECT_ROOT}/test/integration/.flight_server.pid"
PID_CONTROL_PLANE_FILE="${PROJECT_ROOT}/test/integration/.control_plane.pid"

FLIGHT_HOST="${FLIGHT_HOST:-127.0.0.1}"
FLIGHT_PORT="${FLIGHT_PORT:-8815}"

CONTROL_PLANE_HOST="${CONTROL_PLANE_HOST:-127.0.0.1}"
CONTROL_PLANE_PORT="${CONTROL_PLANE_PORT:-8080}"

FLIGHT_ENDPOINT="${FLIGHT_ENDPOINT:-grpc://${FLIGHT_HOST}:${FLIGHT_PORT}}"

log_info() {
    echo "[INFO] $1"
}

log_warn() {
    echo "[WARN] $1" >&2
}

log_error() {
    echo "[ERROR] $1" >&2
}

require_python() {
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 not found. Please install Python 3."
        exit 1
    fi
}

setup_venv() {
    require_python

    if [ ! -d "$VENV_DIR" ]; then
        log_info "Creating Python virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi

    # shellcheck disable=SC1090
    source "$VENV_DIR/bin/activate"

    if ! python3 -c "import pyarrow.flight" 2>/dev/null || ! python3 -c "import duckdb" 2>/dev/null; then
        log_info "Installing Python dependencies..."
        pip install --quiet --upgrade pip
        pip install --quiet -r "${PROJECT_ROOT}/test/integration/requirements.txt"
    fi
}

pid_is_running() {
    local pid_file="$1"
    if [ ! -f "$pid_file" ]; then
        return 1
    fi
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -z "$pid" ]; then
        return 1
    fi
    kill -0 "$pid" 2>/dev/null
}

stop_pid_file() {
    local name="$1"
    local pid_file="$2"
    if [ -f "$pid_file" ]; then
        local pid
        pid="$(cat "$pid_file" 2>/dev/null || true)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping ${name} (PID: ${pid})..."
            kill "$pid" 2>/dev/null || true
        else
            log_info "${name} not running (stale PID file)"
        fi
        rm -f "$pid_file"
    fi
}

wait_for_port() {
    local host="$1"
    local port="$2"
    local name="$3"

    # Requires venv activated (python3 is fine either way, but keep it consistent)
    local attempt=0
    while [ $attempt -lt 30 ]; do
        if python3 - <<PY 2>/dev/null
import socket, sys
s = socket.socket()
s.settimeout(0.2)
try:
    s.connect(("${host}", int("${port}")))
except Exception:
    sys.exit(1)
finally:
    s.close()
PY
        then
            log_info "${name} is ready on ${host}:${port}"
            return 0
        fi
        sleep 0.2
        attempt=$((attempt + 1))
    done

    log_error "${name} failed to start on ${host}:${port}"
    return 1
}

start_control_plane_background() {
    if pid_is_running "$PID_CONTROL_PLANE_FILE"; then
        log_info "Control plane already running (PID: $(cat "$PID_CONTROL_PLANE_FILE"))"
        return 0
    fi

    log_info "Starting mock control plane on ${CONTROL_PLANE_HOST}:${CONTROL_PLANE_PORT}..."
    python3 "${PROJECT_ROOT}/test/integration/control_plane_server.py" \
        --host "$CONTROL_PLANE_HOST" \
        --port "$CONTROL_PLANE_PORT" \
        --flight-endpoint "$FLIGHT_ENDPOINT" &
    echo $! > "$PID_CONTROL_PLANE_FILE"
    wait_for_port "$CONTROL_PLANE_HOST" "$CONTROL_PLANE_PORT" "Control plane"
}

start_flight_background() {
    if pid_is_running "$PID_FLIGHT_FILE"; then
        log_info "Flight server already running (PID: $(cat "$PID_FLIGHT_FILE"))"
        return 0
    fi

    log_info "Starting Flight SQL server on ${FLIGHT_HOST}:${FLIGHT_PORT}..."
    python3 "${PROJECT_ROOT}/test/integration/flight_server.py" \
        --host "$FLIGHT_HOST" \
        --port "$FLIGHT_PORT" &
    echo $! > "$PID_FLIGHT_FILE"
    wait_for_port "$FLIGHT_HOST" "$FLIGHT_PORT" "Flight server"
}

start_servers() {
    local background=false
    local with_control_plane=false

    for arg in "$@"; do
        case "$arg" in
            --background)
                background=true
                ;;
            --control-plane)
                with_control_plane=true
                ;;
            *)
                log_error "Unknown argument: ${arg}"
                log_error "Usage: ./scripts/test-servers.sh start [--background] [--control-plane]"
                exit 1
                ;;
        esac
    done

    setup_venv

    if [ "$background" = true ]; then
        if [ "$with_control_plane" = true ]; then
            start_control_plane_background
        fi
        start_flight_background

        log_info "Environment (use in the shell running tests):"
        echo "export FLIGHT_HOST=${FLIGHT_HOST}"
        echo "export FLIGHT_PORT=${FLIGHT_PORT}"
        if [ "$with_control_plane" = true ]; then
            echo "export CONTROL_PLANE_HOST=${CONTROL_PLANE_HOST}"
            echo "export CONTROL_PLANE_PORT=${CONTROL_PLANE_PORT}"
        fi
        return 0
    fi

    # Foreground mode
    if [ "$with_control_plane" = true ]; then
        start_control_plane_background
        trap 'stop_pid_file "Control plane" "$PID_CONTROL_PLANE_FILE"' EXIT
    fi

    log_info "Starting Flight SQL server in foreground on ${FLIGHT_HOST}:${FLIGHT_PORT}..."
    python3 "${PROJECT_ROOT}/test/integration/flight_server.py" \
        --host "$FLIGHT_HOST" \
        --port "$FLIGHT_PORT"
}

stop_servers() {
    stop_pid_file "Flight server" "$PID_FLIGHT_FILE"
    stop_pid_file "Control plane" "$PID_CONTROL_PLANE_FILE"
}

status_servers() {
    local ok=true

    if pid_is_running "$PID_FLIGHT_FILE"; then
        log_info "Flight server is running (PID: $(cat "$PID_FLIGHT_FILE"))"
    else
        log_warn "Flight server is not running"
        ok=false
    fi

    if pid_is_running "$PID_CONTROL_PLANE_FILE"; then
        log_info "Control plane is running (PID: $(cat "$PID_CONTROL_PLANE_FILE"))"
    else
        log_warn "Control plane is not running"
    fi

    if [ "$ok" = true ]; then
        exit 0
    fi
    exit 1
}

print_env() {
    echo "export FLIGHT_HOST=${FLIGHT_HOST}"
    echo "export FLIGHT_PORT=${FLIGHT_PORT}"
    echo "export CONTROL_PLANE_HOST=${CONTROL_PLANE_HOST}"
    echo "export CONTROL_PLANE_PORT=${CONTROL_PLANE_PORT}"
}

case "${1:-}" in
    start|"")
        shift || true
        start_servers "$@"
        ;;
    stop)
        stop_servers
        ;;
    status)
        status_servers
        ;;
    env)
        print_env
        ;;
    *)
        log_error "Unknown command: ${1}"
        log_error "Usage: ./scripts/test-servers.sh start [--background] [--control-plane] | stop | status | env"
        exit 1
        ;;
esac

