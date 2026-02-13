#!/usr/bin/env bash
# Run full local test suite (unit + integration) with integration lifecycle.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_SERVERS="${PROJECT_ROOT}/scripts/test-servers.sh"
UNITTEST_BIN="${PROJECT_ROOT}/build/release/test/unittest"

INCLUDE_GLOB="${1:-test/*}"
EXCLUDE_GLOB="${2:-~test/sql/roadmap/*}"

cleanup() {
    "${TEST_SERVERS}" stop
}

"${TEST_SERVERS}" start --background --seed
trap cleanup EXIT INT TERM

eval "$("${TEST_SERVERS}" env)"
"${UNITTEST_BIN}" "${INCLUDE_GLOB}" "${EXCLUDE_GLOB}"
