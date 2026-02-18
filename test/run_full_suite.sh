#!/usr/bin/env bash
# Run full local test suite (unit + integration) with integration lifecycle.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_SERVERS="${PROJECT_ROOT}/scripts/test-servers.sh"
UNITTEST_BIN="${PROJECT_ROOT}/build/release/test/unittest"

INCLUDE_GLOB="${1:-test/*}"
EXCLUDE_GLOB="${2:-~test/sql/roadmap/*,~test/sql/token/*}"

start_servers() {
    "${TEST_SERVERS}" start --background --seed
    eval "$("${TEST_SERVERS}" env)"
}

run_phase() {
    local include_glob="$1"
    local exclude_glob="${2:-}"

    start_servers
    if [[ -n "${exclude_glob}" ]]; then
        "${UNITTEST_BIN}" "${include_glob}" "${exclude_glob}"
    else
        "${UNITTEST_BIN}" "${include_glob}"
    fi
    "${TEST_SERVERS}" stop
}

cleanup() {
    "${TEST_SERVERS}" stop || true
}

trap cleanup EXIT INT TERM

# Default suite (unit + integration + caller filters).
run_phase "${INCLUDE_GLOB}" "${EXCLUDE_GLOB}"

# Dedicated token-expiry regression phase.
(
    export DUCKGRES_FLIGHT_SESSION_TOKEN_TTL=100ms
    export DUCKHOG_TOKEN_EXPIRY_TEST=1
    run_phase "test/sql/token/session_token_invalidation_recovery.test_slow"
)
