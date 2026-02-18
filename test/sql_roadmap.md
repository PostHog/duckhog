# SQL Roadmap Suite Plan (DuckHog-Only, DuckLake-Referenced)

Last updated: `2026-02-17`

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
- [`rm10_time_travel_remote.test_slow`](sql/roadmap/rm10_time_travel_remote.test_slow) — [#33](https://github.com/PostHog/duckhog/issues/33)
- [`rm11_table_changes_remote.test_slow`](sql/roadmap/rm11_table_changes_remote.test_slow) — [#34](https://github.com/PostHog/duckhog/issues/34)
- [`rm12_snapshots_remote.test_slow`](sql/roadmap/rm12_snapshots_remote.test_slow) — [#35](https://github.com/PostHog/duckhog/issues/35)
- [`rm13_table_info_remote.test_slow`](sql/roadmap/rm13_table_info_remote.test_slow) — [#36](https://github.com/PostHog/duckhog/issues/36)
- [`rm14_nested_types_remote.test_slow`](sql/roadmap/rm14_nested_types_remote.test_slow) — [#37](https://github.com/PostHog/duckhog/issues/37)
- [`rm16_on_conflict_do_nothing_remote.test_slow`](sql/roadmap/rm16_on_conflict_do_nothing_remote.test_slow) — [#38](https://github.com/PostHog/duckhog/issues/38)
- [`rm17_on_conflict_do_update_remote.test_slow`](sql/roadmap/rm17_on_conflict_do_update_remote.test_slow) — [#39](https://github.com/PostHog/duckhog/issues/39)
- [`rm19_on_conflict_do_nothing_returning_remote.test_slow`](sql/roadmap/rm19_on_conflict_do_nothing_returning_remote.test_slow) — [#40](https://github.com/PostHog/duckhog/issues/40)
- [`rm20_insert_returning_omitted_columns_remote.test_slow`](sql/roadmap/rm20_insert_returning_omitted_columns_remote.test_slow) — [#41](https://github.com/PostHog/duckhog/issues/41)
- [`rm21_insert_partial_returning_remote.test_slow`](sql/roadmap/rm21_insert_partial_returning_remote.test_slow) — [#42](https://github.com/PostHog/duckhog/issues/42)
- [`rm22_insert_values_default_literals_remote.test_slow`](sql/roadmap/rm22_insert_values_default_literals_remote.test_slow) — [#43](https://github.com/PostHog/duckhog/issues/43)
- [`rm23_on_conflict_rewrite_path_remote.test_slow`](sql/roadmap/rm23_on_conflict_rewrite_path_remote.test_slow) — [#44](https://github.com/PostHog/duckhog/issues/44)

Non-test-file targets:
- DML rewriter CTE support (UPDATE/DELETE) — [#45](https://github.com/PostHog/duckhog/issues/45)

Graduated targets (now part of normal integration suite):
- [`insert_remote.test_slow`](sql/queries/insert_remote.test_slow) (RM01)
- [`update_remote.test_slow`](sql/queries/update_remote.test_slow) (RM02)
- [`insert_default_values_remote.test_slow`](sql/queries/insert_default_values_remote.test_slow) (RM18)
- [`insert_returning_remote.test_slow`](sql/queries/insert_returning_remote.test_slow) (RM15)
- [`delete_remote.test_slow`](sql/queries/delete_remote.test_slow) (RM03)
- [`truncate_remote.test_slow`](sql/queries/truncate_remote.test_slow) (RM04) — desugared to DELETE by DuckDB's grammar
- [`ctas_remote.test_slow`](sql/queries/ctas_remote.test_slow) (RM05)
- [`create_view_remote.test_slow`](sql/queries/create_view_remote.test_slow) (RM06)
- [`rename_table_remote.test_slow`](sql/queries/rename_table_remote.test_slow) (RM07)
- [`partition_insert_remote.test_slow`](sql/queries/partition_insert_remote.test_slow) (RM08)
- [`merge_remote.test_slow`](sql/queries/merge_remote.test_slow) (RM09) — [#32](https://github.com/PostHog/duckhog/issues/32)

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
