# SQL Roadmap Suite Plan (DuckHog-Only, DuckLake-Referenced)

Last updated: `2026-02-23`

## Goal

Track DuckLake-aligned SQL feature gaps as a non-gating roadmap suite.

This suite is intended to:
- Provide a stable TODO list of not-yet-supported capabilities.
- Avoid polluting normal unit/integration tests.
- Avoid using coverage percentages as a release gate.

## Scope

Execution target:
- `DuckDB + DuckHog -> Flight SQL -> DuckGres -> DuckLake`

Roadmap tests are SQLLogicTest files under:
- `test/sql/roadmap`

They are run only by:
- `test/run_sql_roadmap.sh`
- `make test-roadmap`

They are **not** part of:
- normal `make test`
- CI pass/fail gating in `.github/workflows/MainDistributionPipeline.yml`

## Execution

```bash
cd "$(git rev-parse --show-toplevel)"
./test/run_sql_roadmap.sh
```

Strict mode (optional):

```bash
./test/run_sql_roadmap.sh --strict
```

Artifacts:
- `test/integration/roadmap/<timestamp>/roadmap_summary.txt`
- `test/integration/roadmap/<timestamp>/roadmap_failing_tests.txt`
- `test/integration/roadmap/<timestamp>/roadmap_unittest_output.txt`

## Roadmap Targets

Each roadmap test file contains one target capability so failures are isolated.

Current targets:
- [`rm14_nested_types_remote.test_slow`](sql/roadmap/rm14_nested_types_remote.test_slow) — [#37](https://github.com/PostHog/duckhog/issues/37)
- [`rm23_on_conflict_rewrite_path_remote.test_slow`](sql/roadmap/rm23_on_conflict_rewrite_path_remote.test_slow) — [#44](https://github.com/PostHog/duckhog/issues/44) — memory catalog only (non-DuckLake)

Non-test-file targets:
- DML rewriter CTE support (UPDATE/DELETE) — [#45](https://github.com/PostHog/duckhog/issues/45)

Graduated targets (now part of normal integration suite):
- [`insert_remote.test_slow`](sql/integration/insert_remote.test_slow) (RM01)
- [`update_remote.test_slow`](sql/integration/update_remote.test_slow) (RM02)
- [`delete_remote.test_slow`](sql/integration/delete_remote.test_slow) (RM03)
- [`truncate_remote.test_slow`](sql/integration/truncate_remote.test_slow) (RM04) — desugared to DELETE by DuckDB's grammar
- [`ctas_remote.test_slow`](sql/integration/ctas_remote.test_slow) (RM05)
- [`create_view_remote.test_slow`](sql/integration/create_view_remote.test_slow) (RM06)
- [`rename_table_remote.test_slow`](sql/integration/rename_table_remote.test_slow) (RM07)
- [`partition_insert_remote.test_slow`](sql/integration/partition_insert_remote.test_slow) (RM08)
- [`merge_remote.test_slow`](sql/integration/merge_remote.test_slow) (RM09) — [#32](https://github.com/PostHog/duckhog/issues/32)
- [`time_travel_remote.test_slow`](sql/integration/time_travel_remote.test_slow) (RM10) — [#33](https://github.com/PostHog/duckhog/issues/33)
- [`table_functions_remote.test_slow`](sql/integration/table_functions_remote.test_slow) (RM11, RM12, RM13) — [#34](https://github.com/PostHog/duckhog/issues/34), [#35](https://github.com/PostHog/duckhog/issues/35), [#36](https://github.com/PostHog/duckhog/issues/36)
- [`insert_default_values_remote.test_slow`](sql/integration/insert_default_values_remote.test_slow) (RM18)

Retired targets:
- RM15 (INSERT RETURNING) — DuckLake does not support RETURNING on any DML verb; chunk-echo was semantically wrong (echoed client input, not server state). Test file kept as `statement error` coverage.
- RM16 (ON CONFLICT DO NOTHING) — [#38](https://github.com/PostHog/duckhog/issues/38) — duplicate of RM23; both tested `hog:memory` with ON CONFLICT DO NOTHING. Blocked by L2 (constraint metadata not synced to local binder). On DuckLake catalogs, additionally blocked because DuckLake will never support PK/UNIQUE constraints ([ducklake#66](https://github.com/duckdb/ducklake/issues/66), [ducklake#290](https://github.com/duckdb/ducklake/issues/290))
- RM17 (ON CONFLICT DO UPDATE) — [#39](https://github.com/PostHog/duckhog/issues/39) — same blockers as RM16; RM23 covers the rewrite path for all ON CONFLICT variants
- RM19 (ON CONFLICT DO NOTHING RETURNING) — [#40](https://github.com/PostHog/duckhog/issues/40) — same as RM16; also blocked by DuckLake lacking RETURNING support
- RM20 (DEFAULT VALUES + RETURNING) — [#41](https://github.com/PostHog/duckhog/issues/41) — subsumes L1 limitation; RETURNING itself is not supported
- RM21 (Partial columns + RETURNING) — [#42](https://github.com/PostHog/duckhog/issues/42) — same as RM20
- RM22 (Explicit columns + RETURNING) — [#43](https://github.com/PostHog/duckhog/issues/43) — same as RM20

DuckLake's upsert alternative is MERGE INTO, which DuckHog already supports (RM09).

## Design Rules

- Keep roadmap tests independent (no cross-file dependencies).
- Keep one primary target per file.
- Use SQLLogicTest assertions only (`statement ok`, `query` expected rows).
- Do not auto-include roadmap tests in normal test globs.

## Completion Criteria

Roadmap run is complete when all artifacts exist and `roadmap_summary.txt` lists:
- total roadmap tests
- passing roadmap tests
- failing roadmap tests (TODO)
- explicit failing file list
