#!/usr/bin/env bash
# Run full local test suite (unit + integration) with integration lifecycle.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_SERVERS="${PROJECT_ROOT}/scripts/test-servers.sh"
UNITTEST_BIN="${PROJECT_ROOT}/build/release/test/unittest"

INCLUDE_GLOB="${1:-test/*}"
if (($# > 1)); then
    EXCLUDE_GLOBS=("${@:2}")
else
    EXCLUDE_GLOBS=("~test/sql/roadmap/*" "~test/sql/token/*")
fi

start_servers() {
    "${TEST_SERVERS}" start --background --seed
    eval "$("${TEST_SERVERS}" env)"
}

run_phase() {
    local include_glob="$1"
    shift
    local exclude_globs=("$@")

    start_servers
    if [[ ${#exclude_globs[@]} -gt 0 ]]; then
        "${UNITTEST_BIN}" "${include_glob}" "${exclude_globs[@]}"
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
run_phase "${INCLUDE_GLOB}" "${EXCLUDE_GLOBS[@]}"

# Dedicated token-expiry regression phase.
(
    export DUCKGRES_FLIGHT_SESSION_TOKEN_TTL=100ms
    export DUCKHOG_TOKEN_EXPIRY_TEST=1
    run_phase "test/sql/token/session_token_invalidation_recovery.test_slow"
)
