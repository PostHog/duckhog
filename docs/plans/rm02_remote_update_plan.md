# Implement RM02 Remote UPDATE (Non-RETURNING) With Strict Validation (TDD-First)

## Status
- [x] Implemented for remote `UPDATE` (non-`RETURNING`) with strict cross-catalog validation.
- [ ] `UPDATE ... RETURNING` deferred: currently rejected with a clear `NotImplemented` error for this Flight backend path.

## Summary
Implement remote `UPDATE` by executing rewritten SQL directly on the Flight backend, not by local rowid-driven mutation.
This gives broad syntax coverage, clean architecture, and low duplication.
Strict pre-validation for cross-catalog references is included.
`UPDATE ... RETURNING` is intentionally not enabled yet on this backend path.

## Design and Architecture

1. Add a reusable SQL rewrite + validation layer for remote DML.
- New module:
  - `src/execution/posthog_dml_rewriter.hpp`
  - `src/execution/posthog_dml_rewriter.cpp`
- Responsibility:
  - Parse `context.GetCurrentQuery()` through the SQL parser as a statement list.
  - Support batch SQL by locating and validating exactly one top-level `UpdateStatement` in the batch.
    - reject zero-update batches
    - reject batches with more than one `UpdateStatement` (ambiguous for a single remote rewrite)
    - reject non-`UpdateStatement` statements that cannot be rewritten for remote execution
    - validate the update shape (CTE/qualified refs) is safely supported
  - Rewrite local attached catalog references (e.g., `remote_flight`) to real remote catalog (e.g., `ducklake`) in table refs and qualified column refs.
  - Reject explicit references to other catalogs (add case to roadmap test as future todo)
  - Build SQL variants for remote update execution/rewrite plumbing.
- Fail-fast cases:
  - no update statement found
  - more than one update statement found
  - unsupported table ref shape during rewrite
  - explicit foreign catalog references
  - unsupported table ref shape during rewrite
  - explicit foreign catalog references

2. Add a dedicated physical operator for remote UPDATE execution.
- New module:
  - `src/execution/posthog_update.hpp`
  - `src/execution/posthog_update.cpp`
- Operator behavior:
  - Source-only operator (`IsSource = true`, no child required).
  - Non-returning path:
    - execute rewritten SQL via `ExecuteUpdate`, return one-row BIGINT count.
  - Returning path is currently not reachable because planner rejects `op.return_chunk` for this backend.
- Transaction handling:
  - always pass `PostHogTransaction::Get(...).remote_txn_id`.
- Error mapping:
  - reuse/update Flight error mapping so query/update failures are surfaced as DuckDB exceptions consistently.

3. Wire catalog planning to the new operator.
- Update:
  - `src/catalog/posthog_catalog.hpp`
  - `src/catalog/posthog_catalog.cpp`
- Changes:
  - Override `PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &)` (3-arg variant) to avoid planning unnecessary local child pipelines.
  - Keep 4-arg override delegating or remove ambiguity cleanly.
  - Build rewritten SQL through the rewriter module and instantiate `PhysicalPostHogUpdate`.
  - Reject `UPDATE ... RETURNING` with a clear `NotImplemented` message until backend/query-shape support is added.

4. Remove binder hard-stop for updates on remote tables.
- Update:
  - `src/catalog/posthog_table_entry.cpp`
- Change:
  - Replace unconditional throw in `BindUpdateConstraints(...)` with permissive behavior (call base implementation).
  - This prevents binder-level rejection while preserving normal DuckDB update binding semantics.

5. Add sources to build.
- Update:
  - `CMakeLists.txt`
- Add new `.cpp` files for rewriter and update operator.

6. Document behavior.
- Update:
  - `README.md` (or `docs/INTERNAL_API.md` if this is where feature scope is tracked)
- Document:
  - supported UPDATE forms
  - strict cross-catalog rule
  - current `RETURNING` limitation behavior

## Public Interfaces / Types Affected

1. `PostHogCatalog` planning interface
- Add explicit override for 3-arg `PlanUpdate(...)` in `src/catalog/posthog_catalog.hpp`.

2. New operator type
- `PhysicalPostHogUpdate` in `src/execution/posthog_update.hpp`.

3. New DML rewriter API
- Function(s) in `src/execution/posthog_dml_rewriter.hpp` returning validated/re-written SQL variants.

## TDD Execution Plan (Red-Green, Incremental)

1. Red: strengthen RM02 baseline.
- Modify `test/sql/roadmap/rm02_update_remote.test_slow` to insert data before update and assert post-update value.
- Run:
  - `./build/release/test/unittest "test/sql/roadmap/rm02_update_remote.test_slow"`
- Expect fail.

2. Green A: non-returning update core.
- Implement rewriter + non-returning operator path.
- Add test file:
  - `test/sql/queries/update_remote.test_slow`
- Include scenarios:
  - simple WHERE update
  - full-table update
  - multi-column set
  - `SET col = DEFAULT`
  - zero-row affected count
  - transaction rollback/commit behavior
- Run targeted tests; expect pass.

3. Red B: returning scenarios (deferred).
- Keep `test/sql/queries/update_returning_remote.test_slow` as an explicit unsupported-behavior assertion.
- Verify it fails with the expected `NotImplemented` message.

4. Green B: returning stream pipeline (follow-up).
- Re-enable backend-compatible `UPDATE ... RETURNING` execution when Flight backend/query-shape support is available.
- Restore positive `RETURNING` coverage at that time.

5. Red/Green C: strict validation.
- Add:
  - `test/sql/queries/update_remote_errors.test_slow`
- Include:
  - explicit non-target catalog reference in `FROM`
  - explicit non-target catalog-qualified column reference
- Implement fail-fast validation messages; re-run; expect pass.

6. Final verification.
- [x] Run focused suite:
  - `./build/release/test/unittest "test/sql/queries/update*.test_slow"`
- [x] Run roadmap regression once for continuity:
  - `./build/release/test/unittest "test/sql/roadmap/rm02_update_remote.test_slow"` (completed before sunset)
- [x] Run broader query smoke for the update target set.

7. Sunset.
- [x] `test/sql/queries/update*.test_slow` now provides equivalent or stronger coverage than the RM02 roadmap test.
- [x] Removed roadmap file `test/sql/roadmap/rm02_update_remote.test_slow`.
- [x] Updated `test/sql_roadmap.md` to move RM02 into graduated targets.

## Scenario Coverage Target
- `UPDATE ... SET ... WHERE ...`
- `UPDATE ... SET ...` (no WHERE)
- multi-column updates
- `DEFAULT` assignments
- arithmetic and expression assignments
- `UPDATE ... FROM ...` (remote-only references)
- affected-row count semantics (non-returning)
- transaction semantics (commit/rollback)
- clear errors for strict cross-catalog violations

## Assumptions and Defaults
- `UPDATE ... RETURNING` is not currently available on this backend path and is intentionally rejected.
- Strict mode is required: explicit references to catalogs other than target remote catalog are rejected early (user-selected).
- Multiple statements in a query are allowed for parsing, but remote rewrite is only applied when the batch contains exactly one `UpdateStatement`; mixed executable batches are rejected for this path.
- Remote backend/query-shape compatibility for `UPDATE ... RETURNING` remains a follow-up item.
- We optimize for correctness and maintainability over local fallback behavior.
