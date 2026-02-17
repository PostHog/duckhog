# DuckLake / Duckgres Upstream Bugs

Bugs discovered during DuckHog integration testing that live in DuckLake or
Duckgres server-side code, not in DuckHog itself.

---

## 1. NULL partition key crashes DuckLakeInsert

**Discovered:** 2026-02-17 (RM08 partition testing)

**Symptom:** INSERT with a NULL value in a partition key column causes a
server-side crash:

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
INSERT INTO t VALUES (NULL, 'boom');  -- crashes
```

**Root cause:** `DuckLakeInsert::AddWrittenFiles` calls `StringValue::Get` on
the partition key value without checking for NULL first.

**Impact:** Data correctness — NULL is a valid partition key value in most
partitioned storage systems (Hive, Iceberg, Delta all support a `__HIVE_DEFAULT_PARTITION__`
or equivalent). Users who INSERT rows with missing partition keys will get an
internal error instead of either a clean rejection or correct storage.

**DuckHog test:** `test/sql/queries/partition_insert_remote.test_slow` — documented
with a comment block explaining the bug. Cannot use `statement error` because
SQLLogicTest treats internal exceptions as test failures regardless.

---

## 2. DECIMAL columns return NULL through Flight SQL

**Discovered:** 2026-02-17 (RM08 partition testing)

**Symptom:** When a table has a `DECIMAL(p,s)` column, all values in that column
are returned as NULL when queried through the Flight SQL interface. The same
data queried directly (not through Flight) returns correct values.

**Reproduction:**

```sql
-- Via Flight SQL (DuckHog remote attach)
CREATE TABLE t(id INT, amount DECIMAL(10,2));
INSERT INTO t VALUES (1, 100.50), (2, 200.75);
SELECT * FROM t;
-- Returns: (1, NULL), (2, NULL)
```

**Root cause:** Likely a type mapping gap in the Arrow/Flight SQL serialization
path. DECIMAL types require specific Arrow decimal encoding (Decimal128/Decimal256)
and the Duckgres Flight SQL layer may not be handling the DuckDB DECIMAL → Arrow
Decimal conversion correctly, resulting in null values on deserialization.

**Impact:** Silent data loss — queries return NULL instead of actual values with
no error. Any table using DECIMAL columns is affected when accessed through
Flight SQL.

**DuckHog workaround:** Integration tests use `DOUBLE` instead of `DECIMAL` for
monetary/numeric columns. See `test/sql/queries/partition_insert_remote.test_slow`
multi-partition table.
