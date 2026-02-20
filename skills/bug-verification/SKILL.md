---
name: bug-verification
description: Rigorously verify suspected bugs before filing issues. Determines provenance (upstream vs DuckHog), severity, and whether a bug is real. Use whenever a test failure, unexpected behavior, or suspected regression needs to be classified before opening an issue or adding to RELATED_BUGS.md.
---

# Bug Verification

## Purpose

Prevent spurious issue filings by systematically verifying every suspected bug.
A bug is not confirmed until it has been reproduced through an independent path
and attributed to a specific component with evidence.

## Principles

1. **Assume your test is wrong first.** Test authoring errors (wrong type specifiers, wrong expected values, missing env setup) are more common than real bugs.
2. **Verify through an independent path.** If you found a bug through DuckHog, reproduce it via psql. If you found it via psql, try the DuckDB CLI directly.
3. **Attribute precisely.** "It doesn't work" is not a bug report. Identify the exact component, function, and line where the failure originates.
4. **Distinguish data bugs from display bugs.** A BOOLEAN rendering as `0`/`1` might be a real type conversion bug or just the wrong format specifier in your test.
5. **Check upstream before filing.** The bug may already be known, fixed, or by design.

## Workflow

For each suspected bug, execute all steps in order. Do not skip steps.

### Step 1: Reproduce in integration tests

Write a minimal, focused SQLLogicTest that isolates the behavior. The test
should do exactly one thing and have clear expected output.

```
# Minimal reproduction — one table, one operation, one assertion
statement ok
CREATE TABLE remote_catalog.schema.t(col TYPE);

statement ok
INSERT INTO remote_catalog.schema.t VALUES (...);

query I
SELECT col FROM remote_catalog.schema.t;
----
expected_value
```

If the test passes, the bug is not real. Stop here.

If the test fails, record the actual vs expected output and continue.

### Step 2: Reproduce via psql (bypass DuckHog)

Connect directly to Duckgres via the Postgres wire protocol and run the
equivalent SQL. This isolates whether the bug is in DuckHog or upstream.

```bash
eval "$(./scripts/test-servers.sh env)"
psql -h $PGHOST -p $PGPORT -U $PGUSER <<'SQL'
-- Run the same operations directly, no DuckHog involved
CREATE TABLE ducklake.schema.t(col TYPE);
INSERT INTO ducklake.schema.t VALUES (...);
SELECT col FROM ducklake.schema.t;
DROP TABLE ducklake.schema.t;
SQL
```

**Decision point:**
- psql shows the same bug -> upstream (DuckLake or Duckgres). Continue to Step 3.
- psql works correctly -> DuckHog bug. Skip to Step 5.

### Step 3: Test on memory catalog (isolate DuckLake vs Duckgres)

If the bug reproduced via psql, test the same operation on the `memory`
catalog to determine if it's DuckLake-specific or a broader Duckgres/Arrow issue.

```sql
-- Via psql, use memory catalog instead of ducklake
CREATE TABLE memory.main.t(col TYPE);
INSERT INTO memory.main.t VALUES (...);
SELECT col FROM memory.main.t;
```

**Decision point:**
- Bug reproduces on memory catalog too -> Duckgres or DuckDB bug (not DuckLake-specific).
- Bug only on DuckLake -> DuckLake bug.

### Step 4: Search upstream repos for existing issues

Before filing, search all relevant repos:

```bash
# DuckLake issues
gh search issues --repo duckdb/ducklake "keyword" --state open
gh search issues --repo duckdb/ducklake "keyword" --state closed

# Duckgres issues
gh search issues --repo PostHog/duckgres "keyword" --state open

# DuckDB issues (for binder/Arrow bugs)
gh search issues --repo duckdb/duckdb "keyword" --state open
```

Also check recent PRs — the fix may already be merged but unreleased:

```bash
gh search prs --repo duckdb/ducklake "keyword" --state merged
```

### Step 5: Trace to source code

Identify the exact code path. For DuckHog bugs, find the file and line:

| Symptom | Likely location |
|---------|-----------------|
| SELECT returns wrong data | `arrow_stream.cpp:Produce()` — SQL generation |
| INSERT loses/corrupts data | `posthog_insert.cpp` — chunk iteration, `posthog_sql_utils.cpp:BuildInsertSQL` |
| RETURNING fails | `posthog_dml_rewriter.cpp` — CTE wrapping, `arrow_value.cpp:ArrowScalarToValue` |
| Identifier/quoting errors | `arrow_stream.cpp:Produce()`, `posthog_sql_utils.cpp:QuoteIdent` |
| Transaction errors | `posthog_transaction_manager.cpp` |
| DDL errors | `posthog_schema_entry.cpp:RenderAlterTableSQL` or `CreateTable`/`CreateIndex` etc. |
| Catalog errors | `posthog_catalog.cpp`, `posthog_schema_set.cpp` |

For upstream bugs, identify the upstream function from stack traces or error messages.

### Step 6: Classify severity

| Severity | Criteria |
|----------|----------|
| Crash | Server crashes, database invalidated, process killed |
| Silent data loss | Data inserted but not retrievable, rows lost, values corrupted to NULL |
| Feature broken | Operation errors when it should work; clean error message |
| Type fidelity | Values technically accessible but wrong type/representation |
| Limitation | Expected behavior gap, documented |

### Step 7: Write verification test

Create a standalone `.test_slow` file that proves the bug exists. This test
should be self-contained and deterministic.

For confirmed bugs, use `statement error` with the expected error substring.
For data bugs where the wrong value is returned, assert the wrong value with
a comment explaining what the correct value should be.

```
# BUG: arrow_stream.cpp does not escape " in column names (D1)
# Expected: SELECT succeeds and returns 2
# Actual: syntax error because SELECT "has"dq" is invalid SQL
statement error
SELECT "has""dq" FROM remote_catalog.schema.t;
----
syntax error
```

### Step 8: Document in RELATED_BUGS.md

Add the bug to `docs/RELATED_BUGS.md` using the established format:

- **Upstream bugs** get a `U#` prefix
- **DuckHog bugs** get a `D#` prefix
- **Limitations** get an `L#` prefix
- **False positives** get a strikethrough with explanation

Include: component, severity, reproduction SQL, root cause analysis,
verification method, workaround, and test file reference.

## Common False Positives

These patterns frequently look like bugs but aren't:

| Symptom | Likely cause |
|---------|-------------|
| BOOLEAN shows as 0/1 | SQLLogicTest `query II` (integer specifier) instead of `query TI` (text) |
| Wrong row count in test | DuckLake metadata accumulation after DROP/CREATE cycles — use `MAX(table_id)` |
| Table already exists | Memory catalog tables persist across test runs — add `DROP TABLE IF EXISTS` |
| Env vars not substituted | Missing `require-env` directives in test file |
| INTERNAL Error in test | SQLLogicTest treats INTERNAL errors as failures even with `statement error` |
| Wrong sort order | Verify expected values match actual alphabetical/numeric order |

## References

- Provenance decision tree: `references/provenance-tree.md`
- Known bugs catalog: `docs/RELATED_BUGS.md`
- Architecture guide (code paths): `CLAUDE.md`
