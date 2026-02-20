#include "catch.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "execution/posthog_sql_utils.hpp"

using namespace duckdb;
using std::string;

namespace {

/// Helper: initialize a DataChunk with the given types and a single row of values.
void InitChunk(DataChunk &chunk, const vector<LogicalType> &types, const vector<Value> &row) {
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t col = 0; col < types.size(); col++) {
		chunk.SetValue(col, 0, row[col]);
	}
	chunk.SetCardinality(1);
}

/// Helper: initialize a DataChunk with the given types and multiple rows.
void InitChunkMultiRow(DataChunk &chunk, const vector<LogicalType> &types, const vector<vector<Value>> &rows) {
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t row_idx = 0; row_idx < rows.size(); row_idx++) {
		for (idx_t col = 0; col < types.size(); col++) {
			chunk.SetValue(col, row_idx, rows[row_idx][col]);
		}
	}
	chunk.SetCardinality(rows.size());
}

const string TABLE = "ducklake.myschema.t";

} // namespace

// ============================================================
// Basic single-row INSERT
// ============================================================

TEST_CASE("Insert SQL builder - single integer column", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER}, {Value::INTEGER(42)});
	auto sql = BuildInsertSQL(TABLE, {"i"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i) VALUES (42);");
}

TEST_CASE("Insert SQL builder - two columns", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER, LogicalType::VARCHAR}, {Value::INTEGER(1), Value("hello")});
	auto sql = BuildInsertSQL(TABLE, {"i", "v"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i, v) VALUES (1, 'hello');");
}

TEST_CASE("Insert SQL builder - three columns mixed types", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER, LogicalType::DOUBLE, LogicalType::BOOLEAN},
	          {Value::INTEGER(10), Value::DOUBLE(3.14), Value::BOOLEAN(true)});
	auto sql = BuildInsertSQL(TABLE, {"i", "d", "b"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i, d, b) VALUES (10, 3.14, true);");
}

// ============================================================
// DEFAULT VALUES path
// ============================================================

TEST_CASE("Insert SQL builder - DEFAULT VALUES", "[duckhog][insert-sql]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {});
	chunk.SetCardinality(1);
	auto sql = BuildInsertSQL(TABLE, {}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t DEFAULT VALUES;");
}

TEST_CASE("Insert SQL builder - DEFAULT VALUES with ON CONFLICT", "[duckhog][insert-sql]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {});
	chunk.SetCardinality(1);
	auto sql = BuildInsertSQL(TABLE, {}, chunk, " ON CONFLICT DO NOTHING");
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t DEFAULT VALUES ON CONFLICT DO NOTHING;");
}

TEST_CASE("Insert SQL builder - multi-row DEFAULT VALUES throws", "[duckhog][insert-sql]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {});
	chunk.SetCardinality(2);
	REQUIRE_THROWS_AS(BuildInsertSQL(TABLE, {}, chunk), NotImplementedException);
}

// ============================================================
// NULL values
// ============================================================

TEST_CASE("Insert SQL builder - NULL integer", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER, LogicalType::INTEGER}, {Value(LogicalType::INTEGER), Value::INTEGER(5)});
	auto sql = BuildInsertSQL(TABLE, {"a", "b"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (a, b) VALUES (NULL, 5);");
}

TEST_CASE("Insert SQL builder - NULL varchar", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::VARCHAR}, {Value(LogicalType::VARCHAR)});
	auto sql = BuildInsertSQL(TABLE, {"v"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (v) VALUES (NULL);");
}

TEST_CASE("Insert SQL builder - all NULLs", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER, LogicalType::VARCHAR},
	          {Value(LogicalType::INTEGER), Value(LogicalType::VARCHAR)});
	auto sql = BuildInsertSQL(TABLE, {"i", "v"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i, v) VALUES (NULL, NULL);");
}

// ============================================================
// String escaping
// ============================================================

TEST_CASE("Insert SQL builder - single quotes in varchar", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::VARCHAR}, {Value("it's a test")});
	auto sql = BuildInsertSQL(TABLE, {"v"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (v) VALUES ('it''s a test');");
}

TEST_CASE("Insert SQL builder - backslash in varchar", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::VARCHAR}, {Value("path\\to\\file")});
	auto sql = BuildInsertSQL(TABLE, {"v"}, chunk);
	REQUIRE(sql.find("path\\to\\file") != string::npos);
}

TEST_CASE("Insert SQL builder - empty string", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::VARCHAR}, {Value("")});
	auto sql = BuildInsertSQL(TABLE, {"v"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (v) VALUES ('');");
}

TEST_CASE("Insert SQL builder - SQL injection in string literal", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::VARCHAR}, {Value("'; DROP TABLE t; --")});
	auto sql = BuildInsertSQL(TABLE, {"v"}, chunk);
	// ToSQLString escapes the single quote — the payload is safely inside a string literal
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (v) VALUES ('''; DROP TABLE t; --');");
}

// ============================================================
// Column name quoting
// ============================================================

TEST_CASE("Insert SQL builder - reserved word column name", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER}, {Value::INTEGER(1)});
	auto sql = BuildInsertSQL(TABLE, {"select"}, chunk);
	REQUIRE(sql.find("\"select\"") != string::npos);
}

TEST_CASE("Insert SQL builder - column name with spaces", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER}, {Value::INTEGER(1)});
	auto sql = BuildInsertSQL(TABLE, {"my column"}, chunk);
	REQUIRE(sql.find("\"my column\"") != string::npos);
}

TEST_CASE("Insert SQL builder - simple column name not quoted", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER}, {Value::INTEGER(1)});
	auto sql = BuildInsertSQL(TABLE, {"id"}, chunk);
	REQUIRE(sql.find("(id)") != string::npos);
}

// ============================================================
// Multi-row INSERT
// ============================================================

TEST_CASE("Insert SQL builder - two rows", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunkMultiRow(chunk, {LogicalType::INTEGER, LogicalType::VARCHAR},
	                  {{Value::INTEGER(1), Value("a")}, {Value::INTEGER(2), Value("b")}});
	auto sql = BuildInsertSQL(TABLE, {"i", "v"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i, v) VALUES (1, 'a'), (2, 'b');");
}

TEST_CASE("Insert SQL builder - three rows single column", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunkMultiRow(chunk, {LogicalType::INTEGER},
	                  {{Value::INTEGER(10)}, {Value::INTEGER(20)}, {Value::INTEGER(30)}});
	auto sql = BuildInsertSQL(TABLE, {"i"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i) VALUES (10), (20), (30);");
}

TEST_CASE("Insert SQL builder - multi-row with NULLs", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunkMultiRow(
	    chunk, {LogicalType::INTEGER, LogicalType::VARCHAR},
	    {{Value::INTEGER(1), Value(LogicalType::VARCHAR)}, {Value(LogicalType::INTEGER), Value("hello")}});
	auto sql = BuildInsertSQL(TABLE, {"i", "v"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i, v) VALUES (1, NULL), (NULL, 'hello');");
}

// ============================================================
// ON CONFLICT clause
// ============================================================

TEST_CASE("Insert SQL builder - ON CONFLICT DO NOTHING", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER}, {Value::INTEGER(1)});
	auto sql = BuildInsertSQL(TABLE, {"i"}, chunk, " ON CONFLICT DO NOTHING");
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i) VALUES (1) ON CONFLICT DO NOTHING;");
}

TEST_CASE("Insert SQL builder - ON CONFLICT with column list", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER, LogicalType::VARCHAR}, {Value::INTEGER(1), Value("a")});
	auto sql = BuildInsertSQL(TABLE, {"i", "v"}, chunk, " ON CONFLICT (i) DO NOTHING");
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i, v) VALUES (1, 'a') ON CONFLICT (i) DO NOTHING;");
}

TEST_CASE("Insert SQL builder - multi-row with ON CONFLICT", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunkMultiRow(chunk, {LogicalType::INTEGER}, {{Value::INTEGER(1)}, {Value::INTEGER(2)}});
	auto sql = BuildInsertSQL(TABLE, {"i"}, chunk, " ON CONFLICT DO NOTHING");
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i) VALUES (1), (2) ON CONFLICT DO NOTHING;");
}

// ============================================================
// Type coverage
// ============================================================

TEST_CASE("Insert SQL builder - BIGINT", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::BIGINT}, {Value::BIGINT(9223372036854775807LL)});
	auto sql = BuildInsertSQL(TABLE, {"big"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (big) VALUES (9223372036854775807);");
}

TEST_CASE("Insert SQL builder - FLOAT", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::FLOAT}, {Value::FLOAT(1.5f)});
	auto sql = BuildInsertSQL(TABLE, {"f"}, chunk);
	REQUIRE(sql.find("1.5") != string::npos);
}

TEST_CASE("Insert SQL builder - BOOLEAN true and false", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunkMultiRow(chunk, {LogicalType::BOOLEAN}, {{Value::BOOLEAN(true)}, {Value::BOOLEAN(false)}});
	auto sql = BuildInsertSQL(TABLE, {"b"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (b) VALUES (true), (false);");
}

TEST_CASE("Insert SQL builder - DATE", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::DATE}, {Value("2026-01-15")});
	auto sql = BuildInsertSQL(TABLE, {"d"}, chunk);
	REQUIRE(sql.find("2026-01-15") != string::npos);
}

TEST_CASE("Insert SQL builder - TIMESTAMP", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::TIMESTAMP}, {Value("2026-01-15 10:30:00")});
	auto sql = BuildInsertSQL(TABLE, {"ts"}, chunk);
	REQUIRE(sql.find("2026-01-15") != string::npos);
	REQUIRE(sql.find("10:30:00") != string::npos);
}

// ============================================================
// Validation / error cases
// ============================================================

TEST_CASE("Insert SQL builder - column count mismatch throws", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER, LogicalType::INTEGER}, {Value::INTEGER(1), Value::INTEGER(2)});
	REQUIRE_THROWS_AS(BuildInsertSQL(TABLE, {"i"}, chunk), InternalException);
}

TEST_CASE("Insert SQL builder - more column names than chunk columns throws", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER}, {Value::INTEGER(1)});
	REQUIRE_THROWS_AS(BuildInsertSQL(TABLE, {"i", "j"}, chunk), InternalException);
}

TEST_CASE("Insert SQL builder - empty chunk produces no row tuples", "[duckhog][insert-sql]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
	chunk.SetCardinality(0);
	auto sql = BuildInsertSQL(TABLE, {"i"}, chunk);
	REQUIRE(sql.find("VALUES ") != string::npos);
	REQUIRE(sql.find("VALUES ;") != string::npos);
}

// ============================================================
// Table name passthrough
// ============================================================

TEST_CASE("Insert SQL builder - quoted table name passthrough", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::INTEGER}, {Value::INTEGER(1)});
	auto sql = BuildInsertSQL("\"my catalog\".\"my schema\".\"my table\"", {"i"}, chunk);
	REQUIRE(sql.find("\"my catalog\".\"my schema\".\"my table\"") != string::npos);
}
