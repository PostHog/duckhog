# Provenance Decision Tree

Use this tree to attribute a suspected bug to the correct component.
Follow the tree top-down; stop at the first terminal node.

```
Suspected bug
│
├─ Does it reproduce in a DuckHog integration test?
│  NO ──> Not a bug (test setup error, env issue, etc.)
│
├─ Does it reproduce via psql (direct Postgres wire protocol, no DuckHog)?
│  │
│  YES ─┐
│       ├─ Does it reproduce on the memory catalog (not just ducklake)?
│       │  YES ──> Duckgres bug or DuckDB bug
│       │          (Arrow Flight serialization, query execution)
│       │  NO  ──> DuckLake bug
│       │          (storage, metadata, partitioning)
│       │
│  NO ──> DuckHog bug
│         Continue to "DuckHog code path identification" below.
│
└─ Is the error an INTERNAL Error or crash?
   YES ──> Check if our code is even reached.
           If DuckDB crashes before our NotImplementedException,
           it's a DuckDB binder/planner bug, not ours.
```

## DuckHog code path identification

When the bug is confirmed as DuckHog, identify which subsystem:

```
Bug is in DuckHog
│
├─ SELECT returns wrong data or errors?
│  ──> arrow_stream.cpp:Produce()
│      Builds the SELECT SQL sent to the remote server.
│      Common issues: identifier quoting, projection pushdown.
│
├─ INSERT loses or corrupts data?
│  ──> posthog_insert.cpp (chunk iteration)
│  ──> posthog_sql_utils.cpp:BuildInsertSQL (SQL generation)
│      Common issues: chunk boundary handling, value escaping.
│
├─ DELETE or UPDATE RETURNING fails?
│  ──> posthog_dml_rewriter.cpp (CTE wrapping)
│  ──> arrow_value.cpp:ArrowScalarToValue (type conversion)
│      Common issues: CTE wrapping rejected, unsupported Arrow types.
│
├─ Transaction error across catalogs?
│  ──> posthog_transaction_manager.cpp
│      Common issues: shared Flight connection, concurrent transactions.
│
├─ DDL operation fails?
│  ──> posthog_schema_entry.cpp
│      Check if NotImplementedException is thrown (expected)
│      vs DuckDB crashes before reaching our code (DuckDB bug).
│
└─ Catalog/schema operation fails?
   ──> posthog_catalog.cpp, posthog_schema_set.cpp
       Common issues: metadata caching, schema refresh.
```

## Verification matrix

For each bug, you need evidence in at least two of these columns:

| Evidence type | What it proves |
|--------------|----------------|
| Integration test failure | Bug exists through DuckHog |
| psql reproduction | Bug exists without DuckHog (upstream) |
| psql success | Bug does NOT exist without DuckHog (ours) |
| Memory catalog test | Bug is not DuckLake-specific |
| Memory catalog success | Bug IS DuckLake-specific |
| Source code trace | Root cause identified |
| Upstream issue search | Whether already known/fixed |

Minimum for filing an upstream issue: integration test + psql reproduction + source trace + issue search.

Minimum for classifying as DuckHog bug: integration test + psql success + source trace.

## Historical false positives

These were initially classified as upstream bugs but turned out to be
DuckHog issues or test errors:

| Initial claim | Actual cause |
|--------------|-------------|
| "DuckLake loses rows on large INSERT" | DuckHog chunk boundary bug in `posthog_insert.cpp`. psql inserts all rows correctly. |
| "BOOLEAN renders as 0/1 through DuckLake" | Test used `query II` (integer format specifier) instead of `query TI`. BOOLEAN works correctly through Arrow Flight. |
| "INSERT RETURNING broken on DuckLake" | DuckHog code at `posthog_catalog.cpp:309` throws NotImplementedException for partial column RETURNING. Not upstream. |
