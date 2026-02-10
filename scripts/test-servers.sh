#!/bin/bash
#===----------------------------------------------------------------------===//
#                         DuckHog DuckDB Extension
#
# Starts/stops integration test servers for direct Duckgres Flight mode.
#
# Usage:
#   ./scripts/test-servers.sh start [--background] [--seed|--no-seed]
#   ./scripts/test-servers.sh stop
#   ./scripts/test-servers.sh status
#   ./scripts/test-servers.sh env
#
# Notes:
# - Starts Duckgres in control-plane mode with both PGwire and Flight listeners.
# - `--seed` is accepted for compatibility and currently a no-op because tests
#   create their own fixtures.
# - `env` prints export commands; use: eval "$(./scripts/test-servers.sh env)"
#===----------------------------------------------------------------------===//

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_DIR="${PROJECT_ROOT}/test/integration"

PID_DUCKGRES_FILE="${TEST_DIR}/.duckgres.pid"

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-15432}"
FLIGHT_HOST="${FLIGHT_HOST:-127.0.0.1}"
FLIGHT_PORT="${FLIGHT_PORT:-8815}"

DUCKHOG_USER="${DUCKHOG_USER:-postgres}"
DUCKHOG_PASSWORD="${DUCKHOG_PASSWORD:-postgres}"
FLIGHT_ENDPOINT="${FLIGHT_ENDPOINT:-grpc+tls://${FLIGHT_HOST}:${FLIGHT_PORT}}"

DUCKGRES_ROOT_DEFAULT="${PROJECT_ROOT}/../duckgres"
DUCKGRES_ROOT="${DUCKGRES_ROOT:-${DUCKGRES_ROOT_DEFAULT}}"
DUCKGRES_BIN="${DUCKGRES_BIN:-${TEST_DIR}/duckgres_server}"
DUCKGRES_DATA_DIR="${DUCKGRES_DATA_DIR:-${TEST_DIR}/duckgres-data}"
DUCKGRES_SOCKET_DIR="${DUCKGRES_SOCKET_DIR:-${TEST_DIR}/duckgres-sockets}"
DUCKGRES_CERT="${DUCKGRES_CERT:-${TEST_DIR}/server.crt}"
DUCKGRES_KEY="${DUCKGRES_KEY:-${TEST_DIR}/server.key}"
DUCKGRES_WORKERS="${DUCKGRES_WORKERS:-4}"

log_info() {
    echo "[INFO] $1"
}

log_warn() {
    echo "[WARN] $1" >&2
}

log_error() {
    echo "[ERROR] $1" >&2
}

require_go() {
    if ! command -v go &> /dev/null; then
        log_error "Go not found. Please install Go."
        exit 1
    fi
}

require_python() {
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 not found. Please install Python 3."
        exit 1
    fi
}

pid_is_running_pid() {
    local pid="$1"
    if [ -z "$pid" ]; then
        return 1
    fi

    if kill -0 "$pid" 2>/dev/null; then
        return 0
    fi

    local kill_output
    kill_output="$(kill -0 "$pid" 2>&1 || true)"
    if [[ "$kill_output" == *"operation not permitted"* ]] || [[ "$kill_output" == *"Operation not permitted"* ]]; then
        return 0
    fi

    if command -v ps >/dev/null 2>&1; then
        if ps -p "$pid" >/dev/null 2>&1; then
            return 0
        fi
    fi

    return 1
}

pid_is_running() {
    local pid_file="$1"
    if [ ! -f "$pid_file" ]; then
        return 1
    fi
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    pid_is_running_pid "$pid"
}

stop_pid_file() {
    local name="$1"
    local pid_file="$2"

    if [ ! -f "$pid_file" ]; then
        return
    fi

    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -z "$pid" ]; then
        rm -f "$pid_file"
        return
    fi

    if ! pid_is_running_pid "$pid"; then
        log_info "${name} not running (stale PID file)"
        rm -f "$pid_file"
        return
    fi

    log_info "Stopping ${name} (PID: ${pid})..."
    if ! kill "$pid" 2>/dev/null; then
        log_warn "Could not signal ${name} (PID: ${pid}); keeping PID file"
        return
    fi

    local attempt=0
    while [ $attempt -lt 40 ]; do
        if ! pid_is_running_pid "$pid"; then
            rm -f "$pid_file"
            return
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    log_warn "${name} did not stop after SIGTERM; sending SIGKILL"
    kill -9 "$pid" 2>/dev/null || true

    attempt=0
    while [ $attempt -lt 10 ]; do
        if ! pid_is_running_pid "$pid"; then
            rm -f "$pid_file"
            return
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    log_warn "${name} is still running (PID: ${pid}); keeping PID file"
}

wait_for_port() {
    local host="$1"
    local port="$2"
    local name="$3"

    require_python
    local attempt=0
    while [ $attempt -lt 50 ]; do
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

ensure_port_free() {
    local host="$1"
    local port="$2"
    local name="$3"

    require_python
    if python3 - <<PY 2>/dev/null
import socket, sys
s = socket.socket()
s.settimeout(0.2)
try:
    s.connect(("${host}", int("${port}")))
except Exception:
    sys.exit(0)
finally:
    s.close()
sys.exit(1)
PY
    then
        return 0
    fi

    log_error "${name} port is already in use on ${host}:${port}"
    return 1
}

build_duckgres() {
    require_go

    if [ ! -d "$DUCKGRES_ROOT" ]; then
        log_error "Duckgres repo not found: $DUCKGRES_ROOT"
        log_error "Set DUCKGRES_ROOT to your duckgres checkout path"
        exit 1
    fi

    mkdir -p "$(dirname "$DUCKGRES_BIN")"
    log_info "Building Duckgres from ${DUCKGRES_ROOT} ..."
    (
        cd "$DUCKGRES_ROOT"
        go build -o "$DUCKGRES_BIN" .
    )
}

start_duckgres() {
    local mode="$1"

    if pid_is_running "$PID_DUCKGRES_FILE"; then
        log_info "Duckgres already running (PID: $(cat "$PID_DUCKGRES_FILE"))"
        return 0
    fi

    ensure_port_free "$PG_HOST" "$PG_PORT" "Duckgres PG"
    ensure_port_free "$FLIGHT_HOST" "$FLIGHT_PORT" "Duckgres Flight"

    mkdir -p "$TEST_DIR" "$DUCKGRES_DATA_DIR" "$DUCKGRES_SOCKET_DIR"

    log_info "Starting Duckgres control-plane..."
    log_info "PG: ${PG_HOST}:${PG_PORT}, Flight: ${FLIGHT_HOST}:${FLIGHT_PORT}"

    if [ "$mode" = "background" ]; then
        nohup "$DUCKGRES_BIN" \
            --mode control-plane \
            --host "$PG_HOST" \
            --port "$PG_PORT" \
            --flight-host "$FLIGHT_HOST" \
            --flight-port "$FLIGHT_PORT" \
            --worker-count "$DUCKGRES_WORKERS" \
            --socket-dir "$DUCKGRES_SOCKET_DIR" \
            --data-dir "$DUCKGRES_DATA_DIR" \
            --cert "$DUCKGRES_CERT" \
            --key "$DUCKGRES_KEY" \
            > "${TEST_DIR}/duckgres.log" 2>&1 < /dev/null &

        echo $! > "$PID_DUCKGRES_FILE"

        wait_for_port "$PG_HOST" "$PG_PORT" "Duckgres PG"
        wait_for_port "$FLIGHT_HOST" "$FLIGHT_PORT" "Duckgres Flight"
        return 0
    fi

    exec "$DUCKGRES_BIN" \
        --mode control-plane \
        --host "$PG_HOST" \
        --port "$PG_PORT" \
        --flight-host "$FLIGHT_HOST" \
        --flight-port "$FLIGHT_PORT" \
        --worker-count "$DUCKGRES_WORKERS" \
        --socket-dir "$DUCKGRES_SOCKET_DIR" \
        --data-dir "$DUCKGRES_DATA_DIR" \
        --cert "$DUCKGRES_CERT" \
        --key "$DUCKGRES_KEY"
}

start_servers() {
    local mode="$1"
    local seed_mode="$2"

    build_duckgres
    start_duckgres "$mode"

    if [ "$seed_mode" = "seed" ]; then
        log_info "Seed requested; no-op (tests create fixtures dynamically)."
    fi

    if [ "$mode" = "background" ]; then
        log_info ""
        log_info "Servers started. For tests, run:"
        log_info "  eval \"\$(./scripts/test-servers.sh env)\""
    fi
}

stop_servers() {
    stop_pid_file "Duckgres" "$PID_DUCKGRES_FILE"
}

status_servers() {
    if pid_is_running "$PID_DUCKGRES_FILE"; then
        echo "Duckgres: running (PID: $(cat "$PID_DUCKGRES_FILE"))"
    else
        echo "Duckgres: stopped"
    fi
}

print_env() {
    cat <<EOV
export FLIGHT_HOST=${FLIGHT_HOST}
export FLIGHT_PORT=${FLIGHT_PORT}
export FLIGHT_ENDPOINT=${FLIGHT_ENDPOINT}
export DUCKHOG_USER=${DUCKHOG_USER}
export DUCKHOG_PASSWORD=${DUCKHOG_PASSWORD}
export PGHOST=${PG_HOST}
export PGPORT=${PG_PORT}
export PGUSER=${DUCKHOG_USER}
export PGPASSWORD=${DUCKHOG_PASSWORD}
EOV
}

main() {
    local cmd="${1:-}"

    case "$cmd" in
        start)
            shift
            local mode="foreground"
            local seed_mode="no-seed"

            while [ $# -gt 0 ]; do
                case "$1" in
                    --background)
                        mode="background"
                        ;;
                    --seed)
                        seed_mode="seed"
                        ;;
                    --no-seed)
                        seed_mode="no-seed"
                        ;;
                    *)
                        log_error "Usage: ./scripts/test-servers.sh start [--background] [--seed|--no-seed]"
                        exit 1
                        ;;
                esac
                shift
            done

            start_servers "$mode" "$seed_mode"
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
            log_error "Usage: ./scripts/test-servers.sh start [--background] [--seed|--no-seed] | stop | status | env"
            exit 1
            ;;
    esac
}

main "$@"
