#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR=""
START_SERVERS=1
KEEP_SERVERS=0
STRICT=0

usage() {
    cat <<USAGE
Usage: test/run_sql_roadmap.sh [options]

Options:
  --output-dir <path>   Output directory for roadmap artifacts
  --no-start            Do not start/stop test servers; assume already running
  --keep-servers        Keep servers running after completion (only with auto-start)
  --strict              Exit non-zero if roadmap tests fail
  -h, --help            Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --no-start)
            START_SERVERS=0
            shift
            ;;
        --keep-servers)
            KEEP_SERVERS=1
            shift
            ;;
        --strict)
            STRICT=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ "$START_SERVERS" -eq 0 && "$KEEP_SERVERS" -eq 1 ]]; then
    echo "--keep-servers has no effect with --no-start" >&2
fi

if [[ -z "$OUTPUT_DIR" ]]; then
    run_ts="$(date +%Y%m%d_%H%M%S)"
    OUTPUT_DIR="$ROOT_DIR/test/integration/roadmap/$run_ts"
fi
mkdir -p "$OUTPUT_DIR"

cleanup() {
    if [[ "$START_SERVERS" -eq 1 && "$KEEP_SERVERS" -eq 0 ]]; then
        "$ROOT_DIR/scripts/test-servers.sh" stop >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if [[ "$START_SERVERS" -eq 1 ]]; then
    "$ROOT_DIR/scripts/test-servers.sh" start --background --seed
fi

eval "$("$ROOT_DIR/scripts/test-servers.sh" env)"
status_output="$("$ROOT_DIR/scripts/test-servers.sh" status || true)"
printf '%s\n' "$status_output"
if [[ "$status_output" != *"Duckgres: running"* ]] || [[ "$status_output" != *"DuckLake infra: running"* ]]; then
    echo "Test servers are not fully running. Start them first or omit --no-start." >&2
    exit 1
fi

UNITTEST_BIN="$ROOT_DIR/build/release/test/unittest"
if [[ ! -x "$UNITTEST_BIN" ]]; then
    echo "unittest binary not found at $UNITTEST_BIN" >&2
    exit 1
fi

ROADMAP_GLOB="test/sql/roadmap/*"
if ! compgen -G "$ROOT_DIR/$ROADMAP_GLOB" > /dev/null; then
    echo "No roadmap SQLLogicTest files found at $ROADMAP_GLOB" >&2
    exit 1
fi

ROADMAP_LOG="$OUTPUT_DIR/roadmap_unittest_output.txt"
ROADMAP_FAILING="$OUTPUT_DIR/roadmap_failing_tests.txt"
ROADMAP_SUMMARY="$OUTPUT_DIR/roadmap_summary.txt"

set +e
"$UNITTEST_BIN" "$ROADMAP_GLOB" 2>&1 | tee "$ROADMAP_LOG"
unit_rc=${PIPESTATUS[0]}
set -e

python3 - "$ROADMAP_LOG" "$ROADMAP_FAILING" "$ROADMAP_SUMMARY" <<'PY'
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
failing_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])
text = log_path.read_text()

failed = []
for line in text.splitlines():
    m = re.match(r"\d+\.\s+(test/sql/roadmap/[^\s]+)", line.strip())
    if m:
        failed.append(m.group(1))

summary = re.search(r"test cases:\s*(\d+)\s*\|\s*(.*)", text)
total = int(summary.group(1)) if summary else 0
failed_count = len(failed)
passed_count = max(total - failed_count, 0)

failing_path.write_text("\n".join(failed) + ("\n" if failed else ""))

with summary_path.open("w") as out:
    out.write("DuckHog SQL Roadmap Summary\n")
    out.write("==========================\n\n")
    out.write(f"Total roadmap tests: {total}\n")
    out.write(f"Passing roadmap tests: {passed_count}\n")
    out.write(f"Failing roadmap tests (TODO): {failed_count}\n\n")

    out.write("Failing roadmap test files\n")
    out.write("--------------------------\n")
    if failed:
        for item in failed:
            out.write(f"{item}\n")
    else:
        out.write("none\n")
PY

echo "Roadmap run completed. Artifacts written to: $OUTPUT_DIR"

if [[ "$STRICT" -eq 1 ]]; then
    exit "$unit_rc"
fi

exit 0
