# Testing (Following DuckLake Pattern)

This directory contains all tests for the DuckHog DuckDB extension, following the [DuckLake testing conventions](https://duckdb.org/dev/sqllogictest/intro.html).

## Quick Start

```bash
# Build the extension first
GEN=ninja make release

# Run the full test suite (unit + integration).
# Integration tests will only run if FLIGHT_HOST/FLIGHT_PORT are set and the server is running.
make test

# Run integration tests (requires Duckgres control-plane Flight listener)
./scripts/test-servers.sh start --background --seed
eval "$(./scripts/test-servers.sh env)"
./build/release/test/unittest "test/sql/queries/*"
./scripts/test-servers.sh stop
```

## Running Tests

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

**Full suite with config file** (optional, mirrors DuckDB test config behavior):

```bash
DUCKDB_TEST_CONFIG=test/configs/flight.json make test
```

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
    └── queries/
        ├── basic_select.test_slow   # Basic queries (requires server)
        ├── arrow_types.test_slow    # Arrow type/encoding coverage (requires server)
        ├── tables.test_slow         # Schema/table operations
        └── projection.test_slow     # Column projection tests
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
