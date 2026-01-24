# PostHog DuckDB Extension - Integration Tests

Integration tests for the PostHog DuckDB extension using a Python-based Flight SQL server.

## Prerequisites

1. **Python 3.8+** with venv support
2. **Built Extension** - Run `make release` first

## Running Tests

```bash
# Build the extension first
GEN=ninja make release

# Run integration tests (creates venv and installs deps automatically)
./test/integration/run_integration_tests.sh
```

The script will automatically:
1. Create a Python virtual environment at `test/integration/.venv`
2. Install required dependencies (`pyarrow`, `duckdb`)
3. Start the Flight SQL test server
4. Run the integration tests
5. Clean up the server

## Test Server

The integration tests use a Python Flight SQL server (`flight_server.py`) backed by DuckDB:

- Full Flight SQL protocol support via `pyarrow.flight`
- DuckDB as the query engine
- Pre-created test tables: `test_data`, `numbers`, `types_test`

### Running the Server Manually

```bash
# Activate the virtual environment (created by run_integration_tests.sh)
source test/integration/.venv/bin/activate

# Start the server
python test/integration/flight_server.py --host 127.0.0.1 --port 8815

# In another terminal, test with DuckDB
./build/release/duckdb -cmd "
LOAD 'build/release/extension/posthog/posthog.duckdb_extension';
ATTACH 'hog:test?token=demo&endpoint=grpc://127.0.0.1:8815' AS remote;
SELECT * FROM remote.main.test_data;
"
```

## Test Coverage

| Test | Description |
|------|-------------|
| Basic SELECT 1 | Simple query execution |
| Query test_data | Remote table access |
| Query numbers | Multi-row results |
| Schema listing | SHOW SCHEMAS FROM remote |
| Table listing | SHOW TABLES FROM remote.main |
| Column projection | SELECT specific columns |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLIGHT_HOST` | `127.0.0.1` | Server hostname |
| `FLIGHT_PORT` | `8815` | Server port |
| `DUCKDB_BIN` | `build/release/duckdb` | DuckDB CLI path |
| `POSTHOG_LOG_LEVEL` | `INFO` | Extension log level |

## CI/CD

Integration tests run automatically in GitHub Actions after the extension build completes. The CI uses the same script, which creates its own virtual environment.
