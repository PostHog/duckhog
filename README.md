# PostHog DuckDB Extension

A DuckDB extension that enables direct SQL queries against PostHog data warehouse.

## Overview

This extension allows you to attach a PostHog data source to DuckDB and query it using standard SQL. Data is transferred efficiently using Apache Arrow's columnar format via the Flight SQL protocol.

## Features

- **Native DuckDB Integration**: Attach PostHog as a remote database using the `hog:` protocol
- **Arrow Flight SQL**: High-performance data transfer using Apache Arrow's Flight protocol
- **Secure Authentication**: Bearer token authentication for secure access

## Quick Start

### Installation

```sql
-- Coming soon: Install from community extensions
INSTALL posthog FROM community;
LOAD posthog;
```

### Usage

```sql
-- Production: Attach via control plane (recommended)
ATTACH 'hog:my_database?token=YOUR_API_TOKEN' AS posthog_db;

-- Development: Direct flight server connection (bypasses control plane)
ATTACH 'hog:my_database?token=YOUR_API_TOKEN&flight_server=grpc://localhost:8815' AS posthog_db;

-- Query your data
SELECT * FROM posthog_db.events LIMIT 10;
```

### Connection String Format

```
hog:<database_name>?token=<api_token>[&control_plane=<url>][&flight_server=<url>]
```

| Parameter | Description | Required |
|-----------|-------------|----------|
| `database_name` | Name of the database to connect to | Yes |
| `token` | API authentication token | Yes |
| `control_plane` | Control plane URL (default: `http://localhost:8080`) | No |
| `flight_server` | Direct Flight SQL server endpoint (dev/testing only, bypasses control plane) | No |

**Connection Modes:**
- **Production (default)**: Uses `control_plane` to obtain a session and Flight endpoint dynamically
- **Development**: Use `flight_server=` to connect directly to a Flight SQL server, bypassing the control plane

## Building from Source

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for detailed build instructions.

### Quick Build

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/PostHog/posthog-duckdb-extension.git
cd posthog-duckdb-extension

# Set up vcpkg (one-time setup)
cd ..
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg && git checkout 23dc124705fcac41cf35c33dd9541f5094a9c19f
./bootstrap-vcpkg.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=$(pwd)/scripts/buildsystems/vcpkg.cmake
cd ../posthog-duckdb-extension

# Note: this repo pins the vcpkg baseline in vcpkg-configuration.json.
# If you use a different vcpkg checkout, vcpkg will still resolve to the pinned baseline.
# To upgrade dependencies, update the baseline commit in vcpkg-configuration.json.

# Build
GEN=ninja make release

# Test
./build/release/duckdb -cmd "LOAD 'build/release/extension/posthog/posthog.duckdb_extension';"
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
