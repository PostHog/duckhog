# Known Bugs & Limitations

Bugs and limitations discovered during DuckHog integration testing, organized
by component. Each entry includes reproduction steps, verification details,
and resolution status.

---

# Upstream Bugs

Issues in DuckLake, Duckgres, or DuckDB — not fixable in DuckHog.

## U1. NULL partition key crashes DuckLakeInsert

| Field | Value |
|-------|-------|
| Component | DuckLake (server-side) |
| Severity | Crash — invalidates database |
| Discovered | 2026-02-17 |
| Upstream issue | Not yet filed |
| Status | **Open** |

**Symptom:** INSERT with NULL in a partition key column crashes the server.
The DuckLake database is invalidated; all subsequent queries fail with
`FATAL Error: database has been invalidated`.

```sql
CREATE TABLE t(pk INT, v VARCHAR);
ALTER TABLE t SET PARTITIONED BY (pk);
INSERT INTO t VALUES (NULL, 'boom');  -- INTERNAL Error: Calling StringValue::Get on a NULL value
```

**Root cause:** `DuckLakeInsert::AddWrittenFiles` calls `StringValue::Get`
without a NULL check.

**Test:** `partition_insert_remote.test_slow` — documented with comment block.
Cannot use `statement error` because SQLLogicTest treats INTERNAL errors as
test failures.

---

## U2. DECIMAL with scale > 0 returns NULL through Arrow Flight

| Field | Value |
|-------|-------|
| Component | Duckgres (Arrow Flight SQL serialization) |
| Severity | Silent data loss |
| Discovered | 2026-02-17 |
| Upstream issue | Not yet filed |
| Status | **Open** |

**Symptom:** `DECIMAL(p, s)` with `s > 0` returns NULL for all values through
Flight SQL. `DECIMAL(p, 0)` works. Postgres wire protocol returns correct values.

```sql
CREATE TABLE t(id INT, s0 DECIMAL(10,0), s2 DECIMAL(10,2));
INSERT INTO t VALUES (1, 42, 42.12);
SELECT * FROM t;
-- Returns: (1, 42, NULL)    -- s2 is NULL
```

**Root cause:** Gap in DuckDB DECIMAL → Arrow Decimal128 conversion when
scale > 0. Type metadata transmits correctly but values are lost.

**Workaround:** Use `DOUBLE` instead of `DECIMAL` in integration tests.

---

## U3. BOOLEAN DEFAULT rejected as non-literal

| Field | Value |
|-------|-------|
| Component | DuckLake |
| Severity | Limitation (clean error) |
| Discovered | 2026-02-17 |
| Upstream issue | [duckdb/ducklake#479](https://github.com/duckdb/ducklake/issues/479) |
| Status | **Fixed** in PR [#571](https://github.com/duckdb/ducklake/pull/571), shipping with DuckDB v1.5 |

`CREATE TABLE t(flag BOOLEAN DEFAULT TRUE)` fails because DuckLake's default
value parser treated `true`/`false` as expressions, not literals.

**Workaround:** Avoid BOOLEAN DEFAULT on DuckLake until DuckDB v1.5.

---

## ~~U4. Large INSERT loses rows~~ → moved to D4

Originally attributed to DuckLake. Verified via psql that DuckLake inserts
3000/3000 rows correctly through the Postgres wire protocol. The row loss
is in DuckHog's chunk handling — see D4 below.

---

## ~~U5. BOOLEAN renders as 0/1 through DuckLake~~ — not a bug

BOOLEAN works correctly. The `0`/`1` was a test authoring error (using
`query II` integer type specifier instead of `query TI`). Verified that
both raw SELECT and GROUP BY return proper `true`/`false` through DuckLake
via Arrow Flight.

---

## ~~U7. MERGE self-join reads own writes~~ — not confirmed

Self-merge (`MERGE INTO t USING t AS s ... SET val = s.val + 1`) produced
`val + 2` when tested via psql with an attached DuckLake catalog, but through
DuckHog's Flight SQL rewrite path, DuckLake correctly returns `val + 1`
(matching plain DuckDB behavior). The +2 behavior may be specific to the
local-attach execution path in DuckLake, not the SQL-over-Flight path we use.

Not filing upstream — needs further investigation outside DuckHog to confirm.

---

## U6. CREATE INDEX on remote catalog crashes DuckDB binder

| Field | Value |
|-------|-------|
| Component | DuckDB (binder) |
| Severity | Crash (INTERNAL Error) |
| Discovered | 2026-02-17 |
| Upstream issue | Not filed — niche catalog interaction |
| Status | **Open** — not worth filing |

**Symptom:** `CREATE INDEX` on a remote table triggers `INTERNAL Error` inside
`IndexBinder::InitCreateIndexInfo` before our `NotImplementedException` at
`posthog_schema_entry.cpp:258` is ever reached.

**Workaround:** Test skipped entirely. SQLLogicTest treats INTERNAL errors as
test failures even with `statement error`.

---

# DuckHog Bugs

Issues in our code — fixable by us.

## D1. Identifier quoting does not escape `"` in column/table names

| Field | Value |
|-------|-------|
| Severity | Query failure (syntax error) |
| Discovered | 2026-02-17 |
| Status | **Open** |

**Location:** `arrow_stream.cpp:57` — `Produce()` builds SELECT SQL with:
```cpp
columns_str += "\"" + columns[i] + "\"";
```
No escaping of `"` within the identifier. A column named `has"dq` produces
`SELECT "has"dq" FROM ...` which is a syntax error on the remote server.

Same pattern at lines 50 (fallback column) and 66–68 (table/schema names).

**Fix:** Replace `"` with `""` inside identifiers before quoting, or use
`KeywordHelper::WriteQuoted`.

**Test:** `identifier_edge_cases_remote.test_slow` — confirmed with
`statement error` + comment.

---

## D2. DELETE/UPDATE RETURNING fails — CTE wrapping rejected

| Field | Value |
|-------|-------|
| Severity | Feature broken on all remote catalogs |
| Discovered | 2026-02-17 |
| Status | **Open** |

**Location:** `posthog_dml_rewriter.cpp:153–154` (DELETE), `:207–208` (UPDATE).

DuckHog wraps RETURNING in a CTE:
```sql
WITH __duckhog_deleted_rows AS (DELETE FROM t WHERE ... RETURNING *) SELECT * FROM __duckhog_deleted_rows
```
Duckgres (and DuckDB generally) rejects this because CTEs require a SELECT
statement, not DELETE/UPDATE.

**Affects:** All remote catalogs (DuckLake and memory). INSERT RETURNING works
because it uses a different code path.

**Fix:** Either send the bare `DELETE ... RETURNING *` and handle the Arrow
result directly, or use a different wrapping strategy.

**Tests:** `returning_types_remote.test_slow`, `delete_remote.test_slow` —
both document with `statement error`.

---

## D3. Cross-catalog JOINs fail with "transaction already active"

| Field | Value |
|-------|-------|
| Severity | Feature limitation |
| Discovered | 2026-02-17 |
| Status | **Open** |

**Location:** `posthog_transaction_manager.cpp:42`

Each attached catalog has its own `PostHogTransactionManager`. When a query
touches two remote catalogs on the same Flight server, the second catalog's
`BeginTransaction` call fails because a transaction is already open on that
server connection.

```sql
SELECT a.v, b.v
FROM ducklake_catalog.schema.t1 a
JOIN memory_catalog.main.t2 b ON a.id = b.id;
-- Error: transaction already active
```

Also affects `INSERT...SELECT` across catalogs and subqueries referencing both.

**Fix:** Share transaction state across catalogs attached to the same Flight
server, or use separate connections per catalog.

**Test:** `join_cross_catalog_remote.test_slow` — documented with
`statement error`.

---

## D4. Large INSERT loses rows at chunk boundaries

| Field | Value |
|-------|-------|
| Severity | Silent data loss |
| Discovered | 2026-02-17 |
| Status | **Open** |

**Symptom:** Multi-chunk INSERTs (1500+ rows) lose ~2–4 rows. `MIN`/`MAX`
values are correct, only `COUNT(*)` is short.

```sql
INSERT INTO remote_table SELECT i FROM range(3000) t(i);
SELECT COUNT(*) FROM remote_table;
-- Returns: 2996-2998 (expected 3000)
```

**Verified not upstream:** Direct psql INSERT into the same DuckLake catalog
returns 3000/3000 rows. The loss is in DuckHog's `BuildInsertSQL` or chunk
iteration in `posthog_insert.cpp`.

**Workaround:** Integration tests use tolerance checks (`COUNT(*) >= 2900`)
and verify `MIN`/`MAX` instead of exact counts.

---

# DuckHog Limitations (by design / not yet implemented)

These are known gaps, not bugs.

## L1. INSERT ... RETURNING with omitted columns

`posthog_catalog.cpp:309` throws `NotImplementedException` when INSERT
RETURNING is used with a partial column list. The INSERT itself works; only
the RETURNING path is missing.

## L2. PK/UNIQUE constraint metadata not synced to client

`DESCRIBE` shows `NULL` for key columns. `information_schema` has 0 constraint
rows. Server-side enforcement works (duplicate INSERT correctly errors).
This is a metadata sync gap — we don't propagate constraint metadata from the
remote catalog to the local binder. Also means `ON CONFLICT` can't work
(binder can't see the PK).
