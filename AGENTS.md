# DuckHog DuckDB Extension Agent Guide (Codex)

## Quick commands
- Build release: `GEN=ninja make release`
- Load extension: `./build/release/duckdb -cmd "LOAD 'build/release/extension/duckhog/duckhog.duckdb_extension';"`

## Working Style
- Prefer small, incremental PRs aligned to deliverables.
- TDD with red-green cycle is required: write/adjust tests first and run them to confirm they fail (red), then implement the minimum code to make them pass (green).
- Keep configs and flags explicit; document defaults in README.
- Provide runbooks for local dev and failure recovery.
- When asked to work on/implement a task in a document, mark the task upon completion

## Tests
- Full suite: `make test` (integration runs only when the Flight server is running and env vars are set)
- Unit tests only: `./build/release/test/unittest "test/sql/*.test" "test/sql/connection/*.test"`
- Integration tests (`duckgres` control-plane Flight):
  - `./scripts/test-servers.sh start --background --seed`
  - `eval "$(./scripts/test-servers.sh env)"`
  - `./build/release/test/unittest "test/sql/queries/*"`
  - `./scripts/test-servers.sh stop`
- "I identified why the server keeps 'disappearing' here: background processes don't persist across separate tool invocations in this environment. I'm now running start + integration tests + stop in one command so the Duckgres process stays alive for the entire test run."

## Repo notes
- Submodules required: `git submodule update --init --recursive`
- Dependencies are managed by vcpkg; ensure `VCPKG_TOOLCHAIN_PATH` is set (see `docs/DEVELOPMENT.md`).
- SQLLogicTest files live under `test/sql`; integration tests use `.test_slow`.
