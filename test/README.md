# Testing (Following DuckLake Pattern)

This directory contains all tests for the DuckHog DuckDB extension, following the [DuckLake testing conventions](https://duckdb.org/dev/sqllogictest/intro.html).

## Quick Start

```bash
# Build the extension first
GEN=ninja make release

# Run the full local test suite (unit + integration).
# This auto-starts Duckgres + DuckLake infra, exports test env vars,
# runs tests, and tears everything down.
# Requires duckgres checkout at ../duckgres (or set DUCKGRES_ROOT).
just test-all

# Run integration tests only (manual server lifecycle)
./scripts/test-servers.sh start --background --seed
eval "$(./scripts/test-servers.sh env)"
./build/release/test/unittest "test/sql/queries/*"
./scripts/test-servers.sh stop

# Run roadmap suite (non-gating, intentionally separate from make test/CI)
./test/run_sql_roadmap.sh
```

## Running Tests

`just test-all` is the default full local suite command and includes
integration server setup/teardown automatically.
It expects duckgres at `../duckgres` by default (override with `DUCKGRES_ROOT`).

### Unit Tests

Unit tests (`.test` files) run without external dependencies:

```bash
# Run only unit tests (exclude .test_slow)
./build/release/test/unittest "test/sql/*.test" "test/sql/connection/*.test" "test/sql/errors/*.test"

# Run specific test file
./build/release/test/unittest "test/sql/duckhog.test"

# Run tests matching a pattern
./build/release/test/unittest "test/sql/connection/*"
```

### Integration Tests

Integration tests (`.test_slow` files) require a running Duckgres control-plane process:

```bash
# Step 1: Start Duckgres control-plane (PG + Flight listeners)
./scripts/test-servers.sh start --seed

# Step 2 (in another terminal): Run integration tests
eval "$(./scripts/test-servers.sh env)"
./build/release/test/unittest "test/sql/queries/*"
```

`test-servers.sh` always waits for PG readiness and auto-detects supported
duckgres Flight flag variants; when a Flight control-plane flag/env is
available, it also waits for Flight readiness.
By default it sets `DUCKGRES_MAX_WORKERS=64` for integration runs (override by
exporting `DUCKGRES_MAX_WORKERS` before `start`).

**Background mode** (for CI or single terminal):

```bash
# Start server in background
./scripts/test-servers.sh start --background --seed

# Run tests
eval "$(./scripts/test-servers.sh env)"
./build/release/test/unittest "test/sql/queries/*"

# Stop server when done
./scripts/test-servers.sh stop
```

**Full local suite with config file** (optional):

```bash
DUCKDB_TEST_CONFIG=test/configs/flight.json just test-all
```

### Roadmap Suite (Non-Gating)

Roadmap tests are SQLLogicTests under `test/sql/roadmap/` and are intentionally
excluded from normal test runs and CI gating.

```bash
# Default non-gating run (always exits 0, writes TODO artifacts)
./test/run_sql_roadmap.sh

# Strict mode (exits non-zero if roadmap targets are still failing)
./test/run_sql_roadmap.sh --strict

# Make target wrapper
make test-roadmap
```

Artifacts are written to `test/integration/roadmap/<timestamp>/`:
- `roadmap_summary.txt`
- `roadmap_failing_tests.txt`
- `roadmap_unittest_output.txt`

### Running Individual Test Files

```bash
# Using unittest
./build/release/test/unittest "test/sql/queries/basic_select.test_slow"

# Using DuckDB CLI directly
./build/release/duckdb -unsigned -cmd "LOAD 'build/release/extension/duckhog/duckhog.duckdb_extension';" < test/sql/queries/basic_select.test_slow
```

## Test Structure

```
test/
├── configs/
│   └── flight.json              # Test configuration for Flight SQL
├── integration/
│   ├── duckgres.log             # Duckgres runtime logs
│   └── duckgres_server          # Built duckgres binary used by harness
└── sql/
    ├── duckhog.test             # Extension loading tests
    ├── connection/
    │   ├── attach.test          # Connection string parsing tests
    │   ├── auth.test            # Authentication parameter validation
    ├── errors/
    │   └── connection_errors.test  # Error message verification
    ├── queries/
        ├── basic_select.test_slow   # Basic queries (requires server)
        ├── arrow_types.test_slow    # Arrow type/encoding coverage (requires server)
        ├── tables.test_slow         # Schema/table operations
        └── projection.test_slow     # Column projection tests
    └── roadmap/
        └── rm*.test_slow            # Non-gating DuckLake-aligned roadmap targets
```

## Test File Conventions

| Extension | Purpose | Server Required |
|-----------|---------|-----------------|
| `.test` | Unit tests (extension loading, parsing) | No |
| `.test_slow` | Integration tests (Flight SQL queries) | Yes |

## Test Configuration

The `test/configs/flight.json` file defines environment variables:

```json
{
  "test_env": [
    {"env_name": "FLIGHT_HOST", "env_value": "127.0.0.1"},
    {"env_name": "FLIGHT_PORT", "env_value": "8815"}
  ]
}
```

## Writing New Tests

### SQLLogicTest Format

```sql
# name: test/sql/queries/my_test.test_slow
# description: Description of what this tests
# group: [queries]

require duckhog

statement ok
ATTACH 'hog:memory?user=${DUCKHOG_USER}&password=${DUCKHOG_PASSWORD}&flight_server=grpc+tls://${FLIGHT_HOST}:${FLIGHT_PORT}&tls_skip_verify=true' AS remote;

query I
SELECT COUNT(*) FROM remote.main.numbers;
----
10
```

### Key Directives

| Directive | Purpose |
|-----------|---------|
| `require duckhog` | Load the extension |
| `statement ok` | Statement should succeed |
| `statement error` | Statement should fail (with optional error message match) |
| `query I` | Query returning INTEGER column |
| `query T` | Query returning TEXT column |
| `query IT` | Query returning INTEGER, TEXT columns |
| `rowsort` | Sort rows before comparison |

See [SQLLogicTest documentation](https://duckdb.org/dev/testing) for more details.

## CI/CD

Integration tests run automatically in GitHub Actions:
1. Build extension
2. Start Duckgres control-plane Flight listener (separate step)
3. Run SQLLogicTests against the server

Roadmap suite is not part of CI pass/fail gating by design.
