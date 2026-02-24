---
name: red-team
description: SQL injection and input validation audit for the DuckHog SQL proxy pipeline. Systematically tests all paths where user-supplied identifiers or values flow into rewritten SQL strings. Use as a routine security check after modifying any rewriter, serializer, or SQL-generation code.
---

# Red Team — SQL Injection Audit

## Purpose

DuckHog rewrites user SQL and forwards it to a remote database over Arrow Flight.
Every rewriter is a potential injection surface. This skill systematically probes
all paths where user input flows into generated SQL strings.

## Invariant

All user-supplied identifiers MUST pass through `KeywordHelper::WriteOptionallyQuoted()`
and all values through `Value::ToSQLString()`. Any path that performs raw string
concatenation with user input is a vulnerability.

## Attack Surfaces

| Surface | Entry point | What to probe |
|---------|------------|---------------|
| INSERT serialization | `posthog_sql_utils.cpp:BuildInsertSQL` | Value escaping: `'`, `''`, `\`, NULL bytes, overlong UTF-8, nested quotes in LIST/STRUCT |
| DELETE rewriter | `posthog_dml_rewriter.cpp` | Table/schema names, WHERE clause expressions, USING clause refs |
| UPDATE rewriter | `posthog_dml_rewriter.cpp` | SET expressions, FROM clause refs, subqueries |
| MERGE rewriter | `posthog_dml_rewriter.cpp` | Source table refs, ON clause, WHEN expressions |
| View rewriter | `posthog_view_rewriter.cpp` | View name, schema, column aliases, query body |
| CTAS rewriter | `posthog_ctas_rewriter.cpp` | Table name, column names, type names |
| Rename table | `posthog_schema_entry.cpp` | Old/new table names |
| Partition alter | `posthog_schema_entry.cpp` | Partition column names |
| Time travel | `RenderAtClauseSQL` | VERSION and TIMESTAMP values |
| ATTACH URI | `posthog_catalog.cpp` | `flight_server`, `user`, `password` connection params |
| Arrow Flight results | `arrow_stream.cpp:Produce` | Column names in projection, schema/table in FROM |

## Workflow

### Step 1: Enumerate injection vectors

For each surface, read the source and identify every point where a string is
built from user-supplied input. Classify each as:

- **Safe**: goes through `WriteOptionallyQuoted()` or `ToSQLString()` or DuckDB's AST serializer
- **Suspect**: string concatenation, `StringUtil::Format`, or manual quoting
- **Vulnerable**: raw user input in SQL string with no escaping

Record findings in a table:

```
| File:Line | Input source | Escaping method | Classification |
```

### Step 2: Generate adversarial test cases

For each suspect or vulnerable path, create SQLLogicTest cases. Use payloads from
`references/payloads.md`.

Test structure:
```
# RED-TEAM: [surface] — [payload category]
# Expected: statement error OR correct escaped output
# Vulnerable if: injected SQL executes, data leaks, or server crashes

statement ok
CREATE TABLE remote_flight.schema."Robert'; DROP TABLE t--"(id INT);

statement ok
INSERT INTO remote_flight.schema."Robert'; DROP TABLE t--" VALUES (1);

query I
SELECT id FROM remote_flight.schema."Robert'; DROP TABLE t--";
----
1

statement ok
DROP TABLE remote_flight.schema."Robert'; DROP TABLE t--";
```

### Step 3: Run tests

```bash
# Unit tests (C++ rewriter tests)
./build/release/test/unittest "[duckhog],test/sql/unit/*"

# Integration tests (full pipeline)
./scripts/test-servers.sh start --background --seed
eval "$(./scripts/test-servers.sh env)"
./build/release/test/unittest "test/sql/integration/*"
./scripts/test-servers.sh stop
```

### Step 4: Classify results

For each test case:

| Result | Classification | Action |
|--------|---------------|--------|
| `statement error` with expected message | Safe — injection rejected | No action |
| Correct output with escaped identifiers | Safe — properly quoted | No action |
| Injected SQL executes | **Vulnerability** | File issue, fix immediately |
| Server crash / segfault | **Vulnerability** | File issue, fix immediately |
| Unexpected error message | Investigate | May be safe but fragile |

### Step 5: Report

Output a summary table:

```
| Surface | Vectors tested | Safe | Suspect | Vulnerable |
```

For each vulnerability found, provide:
- Exact payload that triggers it
- File and line of the unsafe code path
- Suggested fix (which escaping function to use)

## Regression Testing

After fixing a vulnerability, add the adversarial test case to the permanent
test suite under `test/sql/unit/security/` or `test/sql/integration/security/`.
These tests should never be removed.

## References

- Injection payloads: `references/payloads.md`
- Attack surface map: `references/surface-map.md`
