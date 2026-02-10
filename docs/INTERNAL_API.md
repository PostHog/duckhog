# Internal API Notes

This document summarizes the key internal classes that make up the PostHog DuckDB extension.
It is intended for maintainers and contributors.

## Storage Extension

- `PostHogStorageExtension` (`src/storage/posthog_storage.cpp`)
  - Registers the `hog:` protocol for `ATTACH`.
  - Parses connection strings and validates required parameters (`user`/`password`).
  - **Single-catalog attach**: if `hog:<catalog>?user=...&password=...` is provided, creates one `PostHogCatalog` for that remote catalog.
  - **Multi-catalog attach**: if no catalog is provided (`hog:?user=...&password=...`), enumerates remote catalogs via Flight SQL metadata and attaches each as a separate DuckDB database using `<alias>_<catalog>` naming. The primary alias (`AS <alias>`) is a stub catalog with no tables - users must use the prefixed versions for queries.
  - Uses an internal `__remote_catalog=<catalog>` option when attaching secondary catalogs to avoid re-enumeration.

## Connection String

- `ConnectionString` / `PostHogConnectionConfig` (`src/utils/connection_string.cpp`)
  - Parses `<catalog>?user=...&password=...&flight_server=...`.
  - URL-decodes values and stores extra options.
  - Applies default Flight endpoint when missing.
  - Validates `tls_skip_verify` (`true`/`false`) and defaults to secure TLS certificate verification.

## Flight SQL Client

- `PostHogFlightClient` (`src/flight/flight_client.cpp`)
  - Wraps Arrow Flight SQL client logic.
  - Adds HTTP Basic auth headers and exposes metadata APIs (schemas/tables).
  - Provides both table and streaming query execution.

## Catalog + Entries

- `PostHogCatalog` (`src/catalog/posthog_catalog.cpp`)
  - Owns the Flight client, connection state, and schema cache.
  - Lazily loads schemas and exposes them via DuckDB's catalog interface.

- `PostHogStubCatalog` (`src/catalog/posthog_stub_catalog.cpp`)
  - Empty catalog used as the primary alias in multi-catalog mode.
  - Has no schemas or tables - all operations throw helpful errors directing users to the prefixed catalogs.

- `PostHogSchemaEntry` (`src/catalog/posthog_schema_entry.cpp`)
  - Lazily loads tables for a schema with caching/TTL.
  - Creates `PostHogTableEntry` instances from remote schemas.

- `PostHogTableEntry` (`src/catalog/posthog_table_entry.cpp`)
  - Maps remote tables to DuckDB table entries.
  - Supplies the remote scan table function and cached Arrow schema.

## Remote Scan + Arrow Stream

- `PostHogRemoteScan` (`src/catalog/remote_scan.cpp`)
  - Builds bind data and integrates with DuckDB's Arrow scan.
  - Uses projection pushdown by generating a projected SQL query.

- `PostHogArrowStream` (`src/flight/arrow_stream.cpp`)
  - Bridges Flight SQL streaming results into DuckDB's Arrow scan.
  - Provides schema and batch iteration via the C Arrow stream interface.
