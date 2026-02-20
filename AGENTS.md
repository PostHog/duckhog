# DuckHog DuckDB Extension Agent Guide (Codex)

## Quick commands
- Build release: `GEN=ninja make release`
- Load extension: `./build/release/duckdb -cmd "LOAD 'build/release/extension/duckhog/duckhog.duckdb_extension';"`

## Working Style
- Prefer small, maintanable and robust solution, avoid hacky or overgegineered fixes.
- TDD with red-green cycle is required: write/adjust tests first and run them to confirm they fail (red), then implement the minimum code to make them pass (green).
- Keep configs and flags explicit; document defaults in README.
- Provide runbooks for local dev and failure recovery.
- When asked to work on/implement a task in a document, mark the task upon completion
- When creating new branch from origin/main, do not track origin/main. 
- Parallelize using subagents when possible.

## Tests
- Unit tests only: `./build/release/test/unittest "[duckhog],test/sql/unit/*"`
- Integration tests (`duckgres` control-plane Flight):
  - Expects duckgres checkout at `../duckgres` (or set `DUCKGRES_ROOT`)
  - `./scripts/test-servers.sh start --background --seed`
  - `eval "$(./scripts/test-servers.sh env)"`
  - `./build/release/test/unittest "test/sql/integration/*"`
  - `./scripts/test-servers.sh stop`
  - in codex sandbox mode, those must run in a single combined command to avoid being killed
- Full local suite (unit + integration with automatic setup/teardown): `just test-all`
- CI/default extension target: `make test` (from extension-ci-tools)
- Roadmap tests verify expected behavior and are intentionally failing, serving as a checklist for missing features. 

## Repo notes
- Submodules required: `git submodule update --init --recursive`
- Dependencies are managed by vcpkg; ensure `VCPKG_TOOLCHAIN_PATH` is set (see `docs/DEVELOPMENT.md`).
- SQLLogicTest files live under `test/sql`; integration tests use `.test_slow`.
- Avoid C++ tests, always prefer SQLLogic
- Before each commit, run make format-fix and make tidy-check in venv

## Skills
A skill is a set of local instructions to follow that is stored in a `SKILL.md` file.

### Available skills
- `issue-gate`: GitHub issue preflight workflow for new tasks. Use before starting any new implementation task to create/claim/abort based on issue state. (file: `skills/issue-gate/SKILL.md`)

### Trigger rules
- Run `$issue-gate` before starting new implementation work.
- Execute the issue-gate preflight directly with `gh` (per `skills/issue-gate/SKILL.md`) and follow the returned decision contract.
- Stop task execution on `ABORT_IN_PROGRESS` and `ABORT_UNCERTAIN`.
