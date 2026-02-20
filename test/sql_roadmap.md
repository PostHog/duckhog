# SQL Roadmap Suite Plan (DuckHog-Only, DuckLake-Referenced)

Last updated: `2026-02-20`

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
- [`rm16_on_conflict_do_nothing_remote.test_slow`](sql/roadmap/rm16_on_conflict_do_nothing_remote.test_slow) — [#38](https://github.com/PostHog/duckhog/issues/38)
- [`rm17_on_conflict_do_update_remote.test_slow`](sql/roadmap/rm17_on_conflict_do_update_remote.test_slow) — [#39](https://github.com/PostHog/duckhog/issues/39)
- [`rm19_on_conflict_do_nothing_returning_remote.test_slow`](sql/roadmap/rm19_on_conflict_do_nothing_returning_remote.test_slow) — [#40](https://github.com/PostHog/duckhog/issues/40)
- [`rm23_on_conflict_rewrite_path_remote.test_slow`](sql/roadmap/rm23_on_conflict_rewrite_path_remote.test_slow) — [#44](https://github.com/PostHog/duckhog/issues/44)

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
- [`insert_returning_remote.test_slow`](sql/integration/insert_returning_remote.test_slow) (RM15)
- [`insert_default_values_remote.test_slow`](sql/integration/insert_default_values_remote.test_slow) (RM18)
- [`insert_defaults_returning_remote.test_slow`](sql/integration/insert_defaults_returning_remote.test_slow) (RM20, RM21, RM22) — [#41](https://github.com/PostHog/duckhog/issues/41), [#42](https://github.com/PostHog/duckhog/issues/42), [#43](https://github.com/PostHog/duckhog/issues/43); RM20/RM21 closed as limitation L1 (RETURNING + omitted columns requires upstream DuckLake INSERT RETURNING support)

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
