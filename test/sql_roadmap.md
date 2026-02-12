# SQL Roadmap Suite Plan (DuckHog-Only, DuckLake-Referenced)

Last updated: `2026-02-12`

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
- `rm02_update_remote.test_slow`
- `rm03_delete_remote.test_slow`
- `rm04_truncate_remote.test_slow`
- `rm05_ctas_remote.test_slow`
- `rm06_create_view_remote.test_slow`
- `rm07_rename_table_remote.test_slow`
- `rm08_partition_insert_remote.test_slow`
- `rm09_merge_remote.test_slow`
- `rm10_time_travel_remote.test_slow`
- `rm11_table_changes_remote.test_slow`
- `rm12_snapshots_remote.test_slow`
- `rm13_table_info_remote.test_slow`
- `rm14_nested_types_remote.test_slow`
- `rm17_on_conflict_do_update_remote.test_slow`
- `rm19_on_conflict_do_nothing_returning_remote.test_slow`
- `rm20_insert_returning_omitted_columns_remote.test_slow`
- `rm21_insert_partial_returning_remote.test_slow`

Graduated targets (now part of normal integration suite):
- `test/sql/queries/insert_remote.test_slow` (RM01)
- `test/sql/queries/insert_default_values_remote.test_slow` (RM18)
- `test/sql/queries/insert_returning_remote.test_slow` (RM15)
- `test/sql/queries/insert_on_conflict_remote.test_slow` (RM16)

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
