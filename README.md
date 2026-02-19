# DuckHog DuckDB Extension

A DuckDB extension that enables direct SQL queries against PostHog data warehouse.

## Overview

This extension allows you to attach a PostHog data source to DuckDB and query it using standard SQL. Data is transferred efficiently using Apache Arrow's columnar format via the Flight SQL protocol.

## Features

- **Native DuckDB Integration**: Attach PostHog as a remote database using the `hog:` protocol
- **Arrow Flight SQL**: High-performance data transfer using Apache Arrow's Flight protocol
- **Secure Authentication**: Username/password authentication over TLS

## Quick Start

### Installation

```sql
-- Coming soon: Install from community extensions
INSTALL duckhog FROM community;
LOAD duckhog;

-- local dev
./build/release/duckdb -cmd "LOAD 'build/release/extension/duckhog/duckhog.duckdb_extension';"

```

### Usage

```sql
-- Direct Flight SQL attach (Duckgres control-plane Flight endpoint)
ATTACH 'hog:my_database?user=postgres&password=postgres&flight_server=grpc+tls://localhost:8815' AS posthog_db;

-- Attach using server-default catalog resolution when catalog is omitted
ATTACH 'hog:?user=postgres&password=postgres&flight_server=grpc+tls://localhost:8815' AS posthog_db;

-- Query your data
SELECT * FROM posthog_db.events LIMIT 10;

-- Local/dev only: skip TLS certificate verification for self-signed certs
ATTACH 'hog:my_database?user=postgres&password=postgres&flight_server=grpc+tls://localhost:8815&tls_skip_verify=true' AS posthog_dev;
```

### Connection String Format

```
hog:[<catalog>]?user=<username>&password=<password>[&flight_server=<url>][&tls_skip_verify=<true|false>]
```

| Parameter | Description | Required |
|-----------|-------------|----------|
| `catalog` | Remote catalog to attach. If omitted (`hog:?user=...`), DuckHog attaches a single catalog using server-default resolution. | No |
| `user` | Flight SQL username | Yes |
| `password` | Flight SQL password | Yes |
| `flight_server` | Flight SQL server endpoint (default: `grpc+tls://127.0.0.1:8815`) | No |
| `tls_skip_verify` | Disable TLS certificate verification (`true`/`false`, default: `false`). Use only for local/dev self-signed certs. | No |

**Catalog Attach Modes:**
- **Single-catalog attach**: `ATTACH 'hog:<catalog>?user=...&password=...' AS remote;` attaches exactly one remote catalog under the local name `remote`.
- **Catalog-omitted attach**: `ATTACH 'hog:?user=...&password=...' AS remote;` attaches one catalog under `remote` using server-default catalog resolution.

## Building from Source

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for detailed build instructions.

### Quick Build

```bash
# First-time setup (vcpkg/toolchain) is documented in docs/DEVELOPMENT.md.
git submodule update --init --recursive
make dev-setup
GEN=ninja make release

# Smoke test (extension loads)
./build/release/duckdb -cmd "LOAD 'build/release/extension/duckhog/duckhog.duckdb_extension';"

# Full local test suite (unit + integration; integration setup is automatic)
# Requires duckgres checkout at ../duckgres (or set DUCKGRES_ROOT)
just test-all
```

`make dev-setup` creates `.venv/` and installs pinned formatter dependencies
from `requirements-dev.txt`. The project `Makefile` automatically prepends
`.venv/bin` to `PATH`, so `make format-fix` works without manually activating
the virtual environment.

## Architecture

The extension is built on several key components:

- **Storage Extension**: Registers the `hog:` protocol with DuckDB's attach system
- **Arrow Flight SQL Client**: Handles communication with the PostHog Flight server
- **Virtual Catalog**: Exposes remote schemas and tables to DuckDB's query planner
- **Type Conversion**: Translates between Arrow and DuckDB data types

## Remote DML Support

- `INSERT` is supported for remote tables, including `INSERT ... RETURNING` for explicit column lists.
- `UPDATE` is supported for remote tables and executes directly on the Flight SQL backend.
  - `UPDATE ... RETURNING` is currently not supported by the Flight backend path.
  - Explicit references to catalogs other than the attached remote catalog are rejected during rewrite/validation.
- `DELETE` is supported for remote tables, including `WHERE`, `USING`, and `RETURNING` clauses.
- CTE references within `UPDATE` and `DELETE` statements are not yet rewritten to the remote catalog.

## Development Status

| Milestone | Status |
|-----------|--------|
| Storage Extension & Protocol Registration | Complete |
| Arrow Flight SQL Client Integration | Complete |
| Virtual Catalog Implementation | Complete |
| Query Pushdown Optimization | Planned |

## Requirements

- DuckDB 1.4.3+
- C++17 compatible compiler
- CMake 3.10+
- vcpkg for dependency management

## Dependencies

- Apache Arrow (with Flight and Flight SQL)
- gRPC
- Protocol Buffers
- OpenSSL

## Contributing

Contributions are welcome! Please see [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for development setup instructions.

## License

[Add license information]
