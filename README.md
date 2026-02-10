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
```

### Usage

```sql
-- Direct Flight SQL attach (Duckgres control-plane Flight endpoint)
ATTACH 'hog:my_database?user=postgres&password=postgres&flight_server=grpc+tls://localhost:8815' AS posthog_db;

-- Multi-catalog: Auto-discover and attach *all* remote catalogs
-- This creates attachments named: <alias>_<catalog> (e.g. posthog_db_test, posthog_db_alt, ...)
-- Note: the primary alias (posthog_db) is a stub with no tables - use the prefixed versions for queries
ATTACH 'hog:?user=postgres&password=postgres&flight_server=grpc+tls://localhost:8815' AS posthog_db;

-- Query your data
SELECT * FROM posthog_db.events LIMIT 10;
-- (In multi-catalog mode you may prefer: SELECT * FROM posthog_db_<catalog>.events LIMIT 10;)

-- Local/dev only: skip TLS certificate verification for self-signed certs
ATTACH 'hog:my_database?user=postgres&password=postgres&flight_server=grpc+tls://localhost:8815&tls_skip_verify=true' AS posthog_dev;
```

### Connection String Format

```
hog:[<catalog>]?user=<username>&password=<password>[&flight_server=<url>][&tls_skip_verify=<true|false>]
```

| Parameter | Description | Required |
|-----------|-------------|----------|
| `catalog` | Remote catalog to attach. If omitted (`hog:?user=...`), auto-discovers and attaches all remote catalogs. | No |
| `user` | Flight SQL username | Yes |
| `password` | Flight SQL password | Yes |
| `flight_server` | Flight SQL server endpoint (default: `grpc+tls://127.0.0.1:8815`) | No |
| `tls_skip_verify` | Disable TLS certificate verification (`true`/`false`, default: `false`). Use only for local/dev self-signed certs. | No |

**Catalog Attach Modes:**
- **Single-catalog attach**: `ATTACH 'hog:<catalog>?user=...&password=...' AS remote;` attaches exactly one remote catalog under the local name `remote`.
- **Multi-catalog attach**: `ATTACH 'hog:?user=...&password=...' AS remote;` discovers remote catalogs and attaches each as `remote_<catalog>`. The `remote` alias is a stub catalog with no tables - always use the prefixed versions for queries.
  - Cleanup: you must `DETACH` each discovered `remote_<catalog>` separately if you want to remove them all.

## Building from Source

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for detailed build instructions.

### Quick Build

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/PostHog/duckhog.git
cd duckhog

# Set up vcpkg (one-time setup)
cd ..
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg && git checkout 23dc124705fcac41cf35c33dd9541f5094a9c19f
./bootstrap-vcpkg.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=$(pwd)/scripts/buildsystems/vcpkg.cmake
cd ../duckhog

# Note: this repo pins the vcpkg baseline in vcpkg-configuration.json.
# If you use a different vcpkg checkout, vcpkg will still resolve to the pinned baseline.
# To upgrade dependencies, update the baseline commit in vcpkg-configuration.json.

# Build
GEN=ninja make release

# Test
./build/release/duckdb -cmd "LOAD 'build/release/extension/duckhog/duckhog.duckdb_extension';"
```

## Architecture

The extension is built on several key components:

- **Storage Extension**: Registers the `hog:` protocol with DuckDB's attach system
- **Arrow Flight SQL Client**: Handles communication with the PostHog Flight server
- **Virtual Catalog**: Exposes remote schemas and tables to DuckDB's query planner
- **Type Conversion**: Translates between Arrow and DuckDB data types

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
