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
- When creating new branch from origin/main, do not track origin/main. 

## Tests
- Unit tests only: `./build/release/test/unittest "test/sql/*.test" "test/sql/connection/*.test"`
- Integration tests (`duckgres` control-plane Flight):
  - Expects a local `duckgres` checkout at `../duckgres` (or set `DUCKGRES_ROOT`)
  - `./scripts/test-servers.sh start --background --seed`
  - `eval "$(./scripts/test-servers.sh env)"`
  - `./build/release/test/unittest "test/sql/queries/*"`
  - `./scripts/test-servers.sh stop`
- Full local suite (unit + integration with automatic setup/teardown): `just test-all`
- CI/default extension target: `make test` (from extension-ci-tools)
- Roadmap tests verify expected behavior and are intentionally failing, serving as a checklist for missing features. 

## Repo notes
- Submodules required: `git submodule update --init --recursive`
- Dependencies are managed by vcpkg; ensure `VCPKG_TOOLCHAIN_PATH` is set (see `docs/DEVELOPMENT.md`).
- SQLLogicTest files live under `test/sql`; integration tests use `.test_slow`.
