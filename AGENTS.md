# PostHog DuckDB Extension Agent Guide (Codex)

## Quick commands
- Build release: `GEN=ninja make release`
- Load extension: `./build/release/duckdb -cmd "LOAD 'build/release/extension/posthog/posthog.duckdb_extension';"`

## Tests
- Full suite: `make test` (integration runs only when the Flight server is running and env vars are set)
- Unit tests only: `./build/release/test/unittest "test/sql/*.test" "test/sql/connection/*.test"`
- Integration tests:
  - `./scripts/flight-server.sh start --background`
  - `export FLIGHT_HOST=127.0.0.1 FLIGHT_PORT=8815`
  - `./build/release/test/unittest "test/sql/queries/*"`
  - `./scripts/flight-server.sh stop`

## Repo notes
- Submodules required: `git submodule update --init --recursive`
- Dependencies are managed by vcpkg; ensure `VCPKG_TOOLCHAIN_PATH` is set (see `docs/DEVELOPMENT.md`).
- SQLLogicTest files live under `test/sql`; integration tests use `.test_slow`.
