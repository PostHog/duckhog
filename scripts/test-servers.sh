#!/bin/bash
#===----------------------------------------------------------------------===//
#                         PostHog DuckDB Extension
#
# Starts/stops integration test servers.
#
# Usage:
#   ./scripts/test-servers.sh start [--background] [--control-plane] [--seed|--no-seed]
#   ./scripts/test-servers.sh stop
#   ./scripts/test-servers.sh status
#   ./scripts/test-servers.sh env
#
# Notes:
# - The Flight SQL server is Duckling (Go) from `posthog-duckdb-server`.
# - `--control-plane` starts the mock control plane in addition to Duckling.
# - `env` prints export commands; use: eval "$(./scripts/test-servers.sh env)"
#===----------------------------------------------------------------------===//

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PID_DUCKLING_GO_FILE="${PROJECT_ROOT}/test/integration/.duckling_go.pid"
PID_CONTROL_PLANE_FILE="${PROJECT_ROOT}/test/integration/.control_plane.pid"

FLIGHT_HOST="${FLIGHT_HOST:-127.0.0.1}"
FLIGHT_PORT="${FLIGHT_PORT:-8815}"

CONTROL_PLANE_HOST="${CONTROL_PLANE_HOST:-127.0.0.1}"
CONTROL_PLANE_PORT="${CONTROL_PLANE_PORT:-8080}"

FLIGHT_ENDPOINT="${FLIGHT_ENDPOINT:-grpc://${FLIGHT_HOST}:${FLIGHT_PORT}}"

DUCKDB_CLI="${DUCKDB_CLI:-${PROJECT_ROOT}/build/release/duckdb}"
DUCKLING_GO_SEEDED_DB_FILE="${DUCKLING_GO_SEEDED_DB_FILE:-${PROJECT_ROOT}/test/integration/memory.duckdb}"
DUCKLING_GO_BIN="${DUCKLING_GO_BIN:-${PROJECT_ROOT}/test/integration/duckling_go_server}"

DUCKLING_GO_ROOT_DEFAULT="${PROJECT_ROOT}/posthog-duckdb-server"
if [ ! -d "$DUCKLING_GO_ROOT_DEFAULT" ]; then
    DUCKLING_GO_ROOT_DEFAULT="${PROJECT_ROOT}/../posthog-duckdb-server"
fi
DUCKLING_GO_ROOT="${DUCKLING_GO_ROOT:-${DUCKLING_GO_ROOT_DEFAULT}}"
DUCKLING_GO_CONFIG_FILE="${DUCKLING_GO_CONFIG_FILE:-${PROJECT_ROOT}/test/integration/duckling_go.yaml}"
DUCKLING_GO_GOCACHE="${DUCKLING_GO_GOCACHE:-${PROJECT_ROOT}/test/integration/.gocache}"

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

require_go() {
    if ! command -v go &> /dev/null; then
        log_error "Go not found. Please install Go."
        exit 1
    fi
}

require_duckdb_cli() {
    if [ ! -x "$DUCKDB_CLI" ]; then
        log_error "DuckDB CLI not found at: $DUCKDB_CLI"
        log_error "Build the extension first: GEN=ninja make release"
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

    # In sandboxed environments, kill -0 can return EPERM for live processes.
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
    if [ -f "$pid_file" ]; then
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
        while [ $attempt -lt 30 ]; do
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
        return
    fi
}

wait_for_port() {
    local host="$1"
    local port="$2"
    local name="$3"

    require_python
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

start_control_plane_background() {
    if pid_is_running "$PID_CONTROL_PLANE_FILE"; then
        log_info "Control plane already running (PID: $(cat "$PID_CONTROL_PLANE_FILE"))"
        return 0
    fi

    ensure_port_free "$CONTROL_PLANE_HOST" "$CONTROL_PLANE_PORT" "Control plane"

    log_info "Starting mock control plane on ${CONTROL_PLANE_HOST}:${CONTROL_PLANE_PORT}..."
    python3 "${PROJECT_ROOT}/test/integration/control_plane_server.py" \
        --host "$CONTROL_PLANE_HOST" \
        --port "$CONTROL_PLANE_PORT" \
        --flight-endpoint "$FLIGHT_ENDPOINT" &
    echo $! > "$PID_CONTROL_PLANE_FILE"
    wait_for_port "$CONTROL_PLANE_HOST" "$CONTROL_PLANE_PORT" "Control plane"
}

seed_duckling_go_db() {
    require_duckdb_cli

    mkdir -p "$(dirname "$DUCKLING_GO_SEEDED_DB_FILE")"
    rm -f "${DUCKLING_GO_SEEDED_DB_FILE}" "${DUCKLING_GO_SEEDED_DB_FILE}.wal" "${DUCKLING_GO_SEEDED_DB_FILE}.tmp" 2>/dev/null || true

    log_info "Seeding DuckDB database for integration tests: ${DUCKLING_GO_SEEDED_DB_FILE}"
    cat > /tmp/posthog_duckling_seed.sql <<'SQL'
DROP TABLE IF EXISTS test_data;
DROP TABLE IF EXISTS numbers;
DROP TABLE IF EXISTS types_test;
DROP TABLE IF EXISTS decimal_test;
DROP TABLE IF EXISTS nested_test;
DROP TABLE IF EXISTS dictionary_test;
DROP TABLE IF EXISTS run_end_test;

CREATE TABLE test_data AS SELECT 1 AS value;
CREATE TABLE numbers AS SELECT i AS id, 'item_' || i AS name FROM range(1, 11) t(i);
CREATE TABLE types_test AS SELECT
    1 AS int_col,
    CAST(1.5 AS DOUBLE) AS float_col,
    'hello' AS str_col,
    TRUE AS bool_col,
    DATE '2024-01-15' AS date_col;
CREATE TABLE decimal_test AS SELECT
    CAST(1234 AS DECIMAL(4,0)) AS dec_p4,
    CAST(123456789 AS DECIMAL(9,0)) AS dec_p9,
    CAST(123456789012345678 AS DECIMAL(18,0)) AS dec_p18,
    CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)) AS dec_p38,
    CAST(123.45 AS DECIMAL(10,2)) AS dec_p10s2;
CREATE TABLE nested_test AS SELECT
    [1, 2, NULL]::INTEGER[] AS int_list,
    []::INTEGER[] AS empty_list,
    struct_pack(a := 1, b := 'hi') AS simple_struct,
    struct_pack(a := NULL, b := 'bye') AS struct_with_null,
    map(['a','b'], [1,2]) AS str_int_map,
    map(['x'], [NULL::INTEGER]) AS map_with_null;
CREATE TABLE dictionary_test AS
SELECT unnest(['alpha','beta','alpha','gamma','beta'])::VARCHAR AS dict_col;
CREATE TABLE run_end_test AS
SELECT * FROM (VALUES (1),(1),(2),(2),(3),(3)) t(ree_col);
SQL

    "$DUCKDB_CLI" "$DUCKLING_GO_SEEDED_DB_FILE" < /tmp/posthog_duckling_seed.sql >/dev/null
    rm -f /tmp/posthog_duckling_seed.sql 2>/dev/null || true
}

build_duckling_go_binary() {
    require_go

    if [ ! -d "$DUCKLING_GO_ROOT" ]; then
        log_error "Duckling (Go) repo not found at: $DUCKLING_GO_ROOT"
        log_error "Set DUCKLING_GO_ROOT to the posthog-duckdb-server repo root."
        exit 1
    fi

    log_info "Building Duckling (Go) server binary..."
    (cd "$DUCKLING_GO_ROOT" && GOCACHE="$DUCKLING_GO_GOCACHE" go build -o "$DUCKLING_GO_BIN" ./worker/duckling/cmd/duckling)
}

write_duckling_go_config() {
    mkdir -p "$(dirname "$DUCKLING_GO_CONFIG_FILE")"
    cat > "$DUCKLING_GO_CONFIG_FILE" <<YAML
# Duckling Flight SQL Server Configuration (Integration Tests)
server:
  host: "${FLIGHT_HOST}"
  port: ${FLIGHT_PORT}

duckdb:
  path: "${DUCKLING_GO_DB_PATH}"
  read_only: false
  threads: 0
  memory: ""

s3:
  enabled: false
  endpoint: ""
  use_ssl: false
  access_key_id: ""
  secret_access_key: ""
  url_style: ""

ducklake:
  enabled: false
  name: "ducklake"
  catalog: ""
  data_path: ""
  read_only: false
  install: false

session:
  token: "demo"
  ttl: "87600h"

logging:
  level: "info"
  format: "text"

metrics:
  enabled: false
  host: "127.0.0.1"
  port: 0
  path: "/metrics"
YAML
}

start_duckling_go_background() {
    if pid_is_running "$PID_DUCKLING_GO_FILE"; then
        log_info "Duckling (Go) already running (PID: $(cat "$PID_DUCKLING_GO_FILE"))"
        return 0
    fi

    ensure_port_free "$FLIGHT_HOST" "$FLIGHT_PORT" "Duckling (Go)"
    build_duckling_go_binary
    write_duckling_go_config

    log_info "Starting Duckling (Go) Flight SQL server on ${FLIGHT_HOST}:${FLIGHT_PORT}..."
    "$DUCKLING_GO_BIN" -config "$DUCKLING_GO_CONFIG_FILE" &
    echo $! > "$PID_DUCKLING_GO_FILE"
    wait_for_port "$FLIGHT_HOST" "$FLIGHT_PORT" "Duckling (Go)"
}

start_servers() {
    local background=false
    local with_control_plane=false
    local seed=true

    for arg in "$@"; do
        case "$arg" in
            --background)
                background=true
                ;;
            --control-plane)
                with_control_plane=true
                ;;
            --seed)
                seed=true
                ;;
            --no-seed)
                seed=false
                ;;
            *)
                log_error "Unknown argument: ${arg}"
                log_error "Usage: ./scripts/test-servers.sh start [--background] [--control-plane] [--seed|--no-seed]"
                exit 1
                ;;
        esac
    done

    if [ "$seed" = true ]; then
        DUCKLING_GO_DB_PATH="$DUCKLING_GO_SEEDED_DB_FILE"
        seed_duckling_go_db
    else
        DUCKLING_GO_DB_PATH=":memory:"
    fi

    if [ "$background" = true ]; then
        if [ "$with_control_plane" = true ]; then
            start_control_plane_background
        fi
        start_duckling_go_background

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

    require_go
    write_duckling_go_config
    log_info "Starting Duckling (Go) Flight SQL server in foreground on ${FLIGHT_HOST}:${FLIGHT_PORT}..."
    build_duckling_go_binary
    "$DUCKLING_GO_BIN" -config "$DUCKLING_GO_CONFIG_FILE"
}

stop_servers() {
    stop_pid_file "Duckling (Go)" "$PID_DUCKLING_GO_FILE"
    stop_pid_file "Control plane" "$PID_CONTROL_PLANE_FILE"
}

status_servers() {
    if pid_is_running "$PID_DUCKLING_GO_FILE"; then
        log_info "Duckling (Go) is running (PID: $(cat "$PID_DUCKLING_GO_FILE"))"
    else
        log_warn "Duckling (Go) is not running"
    fi

    if pid_is_running "$PID_CONTROL_PLANE_FILE"; then
        log_info "Control plane is running (PID: $(cat "$PID_CONTROL_PLANE_FILE"))"
    else
        log_warn "Control plane is not running"
    fi

    if pid_is_running "$PID_DUCKLING_GO_FILE"; then
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
