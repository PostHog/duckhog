# PostHog DuckDB Extension Agent Guide (Codex)

## Quick commands
- Build release: `GEN=ninja make release`
- Load extension: `./build/release/duckdb -cmd "LOAD 'build/release/extension/posthog/posthog.duckdb_extension';"`

## Working Style
- Prefer small, incremental PRs aligned to deliverables.
- TDD with red-green cycle is required: write/adjust tests first and run them to confirm they fail (red), then implement the minimum code to make them pass (green).
- Keep configs and flags explicit; document defaults in README.
- Provide runbooks for local dev and failure recovery.
- When asked to work on/implement a task in a document, mark the task upon completion

## Tests
- Full suite: `make test` (integration runs only when the Flight server is running and env vars are set)
- Unit tests only: `./build/release/test/unittest "test/sql/*.test" "test/sql/connection/*.test"`
- Integration tests:
  - `./scripts/test-servers.sh start --background`
  - `eval "$(./scripts/test-servers.sh env)"`
  - `./build/release/test/unittest "test/sql/queries/*"`
  - `./scripts/test-servers.sh stop`

## Repo notes
- Submodules required: `git submodule update --init --recursive`
- Dependencies are managed by vcpkg; ensure `VCPKG_TOOLCHAIN_PATH` is set (see `docs/DEVELOPMENT.md`).
- SQLLogicTest files live under `test/sql`; integration tests use `.test_slow`.
