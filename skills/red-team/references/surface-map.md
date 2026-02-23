# Attack Surface Map

Maps each source file to the user inputs it handles and the escaping method used.

## SQL Generation

| File | Function | User input | Escaping | Notes |
|------|----------|-----------|----------|-------|
| `posthog_sql_utils.cpp` | `BuildInsertSQL` | Column names, values | `WriteOptionallyQuoted`, `ToSQLString` | Values go through DuckDB's serializer |
| `posthog_sql_utils.cpp` | `QuoteIdent` | Identifiers | `WriteOptionallyQuoted` | Wraps DuckDB's keyword-aware quoting |
| `posthog_dml_rewriter.cpp` | `RewriteDelete` | Full SQL statement | DuckDB parser → AST → serializer | Parses then re-serializes; identifiers rewritten |
| `posthog_dml_rewriter.cpp` | `RewriteUpdate` | Full SQL statement | DuckDB parser → AST → serializer | Same pattern as delete |
| `posthog_dml_rewriter.cpp` | `RewriteMerge` | Full SQL statement | DuckDB parser → AST → serializer | Same pattern |
| `posthog_view_rewriter.cpp` | `RewriteCreateView` | View name, schema, query | DuckDB parser → AST → serializer | |
| `posthog_ctas_rewriter.cpp` | `RewriteCTAS` | Table name, columns, types | DuckDB parser → AST → serializer | |

## Identifier Handling

| File | Function | User input | Escaping | Notes |
|------|----------|-----------|----------|-------|
| `posthog_schema_entry.cpp` | `RenderAlterTableSQL` | Table name, new name, partition keys | `WriteOptionallyQuoted` | |
| `arrow_stream.cpp` | `Produce` | Column names (projection), table ref | `WriteOptionallyQuoted` | Builds SELECT for remote execution |

## Value Handling

| File | Function | User input | Escaping | Notes |
|------|----------|-----------|----------|-------|
| `posthog_sql_utils.cpp` | `BuildInsertSQL` | All column values | `Value::ToSQLString()` | Handles strings, numbers, NULL, LIST, etc. |
| `RenderAtClauseSQL` | Time travel values | VERSION (int), TIMESTAMP (string) | Integer pass-through, string single-quote escaped | Manual `'` → `''` escaping for non-integral types |

## Connection Handling

| File | Function | User input | Escaping | Notes |
|------|----------|-----------|----------|-------|
| `posthog_catalog.cpp` | Constructor | ATTACH URI params | URI parsing | `flight_server`, `user`, `password`, `tls_skip_verify` |

## Higher-Risk Patterns

These are areas where the escaping is not fully delegated to DuckDB's AST:

1. **`RenderAtClauseSQL`** — Manual single-quote escaping for timestamp strings.
   Risk: if a new type is added that isn't integral and doesn't get quoted.

2. **`BuildInsertSQL`** — Relies on `Value::ToSQLString()` for all types.
   Risk: if a new DuckDB type has a broken `ToSQLString()` implementation.

3. **CTE rewriting in DML** — The rewritten SQL includes catalog references
   substituted by string replacement. Risk: catalog name containing SQL.
   Mitigated by: DuckDB's AST serializer handles the catalog name as an identifier.
