# Upstream Bugs & Limitations

Bugs and limitations discovered during DuckHog integration testing that live in
upstream components (DuckLake, Duckgres), not in DuckHog itself.

Each entry includes verification details and upstream issue status.

---

## 1. NULL partition key crashes DuckLakeInsert

| Field | Value |
|-------|-------|
| Component | DuckLake (server-side) |
| Severity | Crash (INTERNAL Error) |
| Discovered | 2026-02-17 (RM08 partition testing) |
| Upstream issue | None filed — not found in duckdb/ducklake issues |
| Status | **Open / unfixed** |

**Symptom:** INSERT with a NULL value in a partition key column causes a
server-side crash. The DuckLake database is invalidated and requires restart.

```
INTERNAL Error: Calling StringValue::Get on a NULL value

Stack trace (relevant frames):
  StringValue::Get
  DuckLakeInsert::AddWrittenFiles
  DuckLakeInsert::Sink
```

**Reproduction:**

```sql
CREATE TABLE t(pk INT, v VARCHAR);
ALTER TABLE t SET PARTITIONED BY (pk);
INSERT INTO t VALUES (NULL, 'boom');  -- crashes, invalidates database
```

**Verification:** Reproduced 2026-02-17 against Duckgres with DuckLake catalog.
Crash invalidates the entire database — subsequent queries on the same server
return `FATAL Error: database has been invalidated because of a previous fatal error`.

**Root cause:** `DuckLakeInsert::AddWrittenFiles` calls `StringValue::Get` on
the partition key value without a NULL check.

**Impact:** NULL is a valid partition key in Hive, Iceberg, and Delta
(`__HIVE_DEFAULT_PARTITION__` or equivalent). Users who INSERT rows with missing
partition keys get a server crash instead of clean rejection or correct storage.

**DuckHog test:** `test/sql/queries/partition_insert_remote.test_slow` — documented
with comment block. Cannot use `statement error` because SQLLogicTest treats
internal exceptions as test failures.

---

## 2. DECIMAL with non-zero scale returns NULL through Arrow Flight

| Field | Value |
|-------|-------|
| Component | Duckgres / Arrow Flight SQL serialization |
| Severity | Silent data loss |
| Discovered | 2026-02-17 (RM08 partition testing) |
| Upstream issue | None filed — not found in PostHog/duckgres or duckdb/ducklake issues |
| Status | **Open / unfixed** |

**Symptom:** `DECIMAL(p, s)` columns with `s > 0` return NULL for all values
when queried through Arrow Flight SQL. `DECIMAL(p, 0)` works correctly. The
Postgres wire protocol returns correct values for the same data.

**Reproduction:**

```sql
-- Through Flight SQL (DuckHog remote attach)
CREATE TABLE t(id INT, s0 DECIMAL(10,0), s1 DECIMAL(10,1), s2 DECIMAL(10,2));
INSERT INTO t VALUES (1, 42, 42.1, 42.12);
SELECT * FROM t;
-- Returns: (1, 42, NULL, NULL)
--                  ^ok  ^null ^null
```

```sql
-- Through Postgres wire protocol (psql): all values correct
-- id | s0 | s1   | s2
-- 1  | 42 | 42.1 | 42.12
```

**Verification:**
- Reproduced on DuckLake catalog and memory catalog — not DuckLake-specific.
- `DECIMAL(10,0)` returns correct values; `DECIMAL(10,1)`, `(10,2)`, `(10,3)`,
  `(18,6)`, `(3,2)` all return NULL.
- `HUGEINT` (mapped to `DECIMAL(38,0)`) works because scale is 0.
- Postgres wire protocol returns correct values for the same data.
- Conclusion: bug is in Arrow Flight serialization path in Duckgres.

**Root cause:** Likely a gap in DuckDB DECIMAL → Arrow Decimal128/256
conversion when scale > 0. The type metadata is transmitted correctly
(column shows `decimal(10,2)`) but the actual values are lost.

**DuckHog workaround:** Integration tests use `DOUBLE` instead of `DECIMAL`.

---

## 3. BOOLEAN DEFAULT rejected as non-literal

| Field | Value |
|-------|-------|
| Component | DuckLake |
| Severity | Limitation (error, not crash) |
| Discovered | 2026-02-17 (integration test expansion) |
| Upstream issue | [duckdb/ducklake#479](https://github.com/duckdb/ducklake/issues/479) |
| Status | **Fixed** in DuckLake PR [#571](https://github.com/duckdb/ducklake/pull/571), shipping with DuckDB v1.5 |

**Symptom:** `CREATE TABLE` with `BOOLEAN DEFAULT TRUE` (or `FALSE`) fails:

```
Invalid Error: Only literals (e.g. 42 or 'hello world') are supported
as default values
```

**Reproduction:**

```sql
CREATE TABLE t(id INT, flag BOOLEAN DEFAULT TRUE);
-- Error: Only literals (e.g. 42 or 'hello world') are supported as default values
```

**Root cause:** DuckLake's default value parser only recognized numeric and
string literals. Boolean `true`/`false` were treated as expressions rather
than literals.

**DuckHog workaround:** Integration tests avoid BOOLEAN DEFAULT on DuckLake
catalogs. Will be resolvable once DuckDB v1.5 ships.

---

## Not upstream bugs (DuckHog issues)

These were initially suspected as upstream bugs but are actually DuckHog
limitations:

### INSERT ... RETURNING with omitted columns

Partial column INSERT works fine on both DuckLake and memory catalogs. The
failure only occurs when combined with `RETURNING`, and the error comes from
DuckHog code at `posthog_catalog.cpp:309`:

```
Not Implemented Error: INSERT ... RETURNING with omitted/default columns
is not yet implemented
```

### PK/UNIQUE constraint metadata not visible to client

`DESCRIBE` shows `NULL` for the key column and `information_schema` has 0
constraint rows, even when the remote catalog has PK/UNIQUE constraints.
Server-side enforcement works (duplicate INSERT correctly errors with
"Duplicate key"). This is a DuckHog metadata sync gap — we don't propagate
constraint metadata from the remote catalog to the local binder.
