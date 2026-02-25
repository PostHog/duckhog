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
// LIST type coverage
// ============================================================

TEST_CASE("Insert SQL builder - LIST of integers", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::LIST(LogicalType::INTEGER)},
	          {Value::LIST(LogicalType::INTEGER, {Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)})});
	auto sql = BuildInsertSQL(TABLE, {"l"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (l) VALUES ([1, 2, 3]);");
}

TEST_CASE("Insert SQL builder - empty LIST", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::LIST(LogicalType::INTEGER)}, {Value::LIST(LogicalType::INTEGER, {})});
	auto sql = BuildInsertSQL(TABLE, {"l"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (l) VALUES ([]);");
}

TEST_CASE("Insert SQL builder - LIST with NULLs", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::LIST(LogicalType::INTEGER)},
	          {Value::LIST(LogicalType::INTEGER, {Value::INTEGER(1), Value(LogicalType::INTEGER), Value::INTEGER(3)})});
	auto sql = BuildInsertSQL(TABLE, {"l"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (l) VALUES ([1, NULL, 3]);");
}

TEST_CASE("Insert SQL builder - LIST of VARCHAR", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::LIST(LogicalType::VARCHAR)},
	          {Value::LIST(LogicalType::VARCHAR, {Value("a"), Value("b")})});
	auto sql = BuildInsertSQL(TABLE, {"l"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (l) VALUES (['a', 'b']);");
}

TEST_CASE("Insert SQL builder - NULL LIST value", "[duckhog][insert-sql]") {
	DataChunk chunk;
	InitChunk(chunk, {LogicalType::LIST(LogicalType::INTEGER)}, {Value(LogicalType::LIST(LogicalType::INTEGER))});
	auto sql = BuildInsertSQL(TABLE, {"l"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (l) VALUES (NULL);");
}

TEST_CASE("Insert SQL builder - multi-row with LIST", "[duckhog][insert-sql]") {
	DataChunk chunk;
	auto list_type = LogicalType::LIST(LogicalType::INTEGER);
	InitChunkMultiRow(chunk, {LogicalType::INTEGER, list_type},
	                  {{Value::INTEGER(1), Value::LIST(LogicalType::INTEGER, {Value::INTEGER(10), Value::INTEGER(20)})},
	                   {Value::INTEGER(2), Value::LIST(LogicalType::INTEGER, {Value::INTEGER(30)})}});
	auto sql = BuildInsertSQL(TABLE, {"i", "l"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (i, l) VALUES (1, [10, 20]), (2, [30]);");
}

// ============================================================
// STRUCT type coverage
// ============================================================

TEST_CASE("Insert SQL builder - basic STRUCT", "[duckhog][insert-sql]") {
	auto struct_type = LogicalType::STRUCT({{"i", LogicalType::INTEGER}, {"j", LogicalType::INTEGER}});
	DataChunk chunk;
	InitChunk(chunk, {struct_type}, {Value::STRUCT({{"i", Value::INTEGER(10)}, {"j", Value::INTEGER(20)}})});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (s) VALUES ({'i': 10, 'j': 20});");
}

TEST_CASE("Insert SQL builder - STRUCT with VARCHAR fields", "[duckhog][insert-sql]") {
	auto struct_type = LogicalType::STRUCT({{"name", LogicalType::VARCHAR}, {"city", LogicalType::VARCHAR}});
	DataChunk chunk;
	InitChunk(chunk, {struct_type}, {Value::STRUCT({{"name", Value("alice")}, {"city", Value("NYC")}})});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (s) VALUES ({'name': 'alice', 'city': 'NYC'});");
}

TEST_CASE("Insert SQL builder - NULL STRUCT", "[duckhog][insert-sql]") {
	auto struct_type = LogicalType::STRUCT({{"i", LogicalType::INTEGER}, {"j", LogicalType::INTEGER}});
	DataChunk chunk;
	InitChunk(chunk, {struct_type}, {Value(struct_type)});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (s) VALUES (NULL);");
}

TEST_CASE("Insert SQL builder - STRUCT with NULL fields", "[duckhog][insert-sql]") {
	auto struct_type = LogicalType::STRUCT({{"i", LogicalType::INTEGER}, {"j", LogicalType::INTEGER}});
	DataChunk chunk;
	InitChunk(chunk, {struct_type}, {Value::STRUCT({{"i", Value(LogicalType::INTEGER)}, {"j", Value::INTEGER(42)}})});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (s) VALUES ({'i': NULL, 'j': 42});");
}

TEST_CASE("Insert SQL builder - nested STRUCT", "[duckhog][insert-sql]") {
	auto inner_type = LogicalType::STRUCT({{"x", LogicalType::INTEGER}, {"y", LogicalType::INTEGER}});
	auto outer_type = LogicalType::STRUCT({{"inner_s", inner_type}, {"label", LogicalType::VARCHAR}});
	DataChunk chunk;
	InitChunk(chunk, {outer_type},
	          {Value::STRUCT({{"inner_s", Value::STRUCT({{"x", Value::INTEGER(1)}, {"y", Value::INTEGER(2)}})},
	                          {"label", Value("origin")}})});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (s) VALUES ({'inner_s': {'x': 1, 'y': 2}, 'label': 'origin'});");
}

TEST_CASE("Insert SQL builder - STRUCT with LIST field", "[duckhog][insert-sql]") {
	auto struct_type =
	    LogicalType::STRUCT({{"tags", LogicalType::LIST(LogicalType::VARCHAR)}, {"count", LogicalType::INTEGER}});
	DataChunk chunk;
	InitChunk(chunk, {struct_type},
	          {Value::STRUCT({{"tags", Value::LIST(LogicalType::VARCHAR, {Value("a"), Value("b")})},
	                          {"count", Value::INTEGER(2)}})});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (s) VALUES ({'tags': ['a', 'b'], 'count': 2});");
}

TEST_CASE("Insert SQL builder - STRUCT with single-quote in field name", "[duckhog][insert-sql]") {
	auto struct_type = LogicalType::STRUCT({{"it's", LogicalType::INTEGER}});
	DataChunk chunk;
	InitChunk(chunk, {struct_type}, {Value::STRUCT({{"it's", Value::INTEGER(1)}})});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	// Field names are single-quoted in struct literals
	REQUIRE(sql.find("'it''s': 1") != string::npos);
}

// ============================================================
// MAP type coverage
// ============================================================

TEST_CASE("Insert SQL builder - basic MAP", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	DataChunk chunk;
	InitChunk(chunk, {map_type},
	          {Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {Value("a")}, {Value::INTEGER(1)})});
	auto sql = BuildInsertSQL(TABLE, {"m"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (m) VALUES (MAP {'a': 1});");
}

TEST_CASE("Insert SQL builder - MAP with multiple entries", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	DataChunk chunk;
	InitChunk(chunk, {map_type},
	          {Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {Value("a"), Value("b")},
	                      {Value::INTEGER(1), Value::INTEGER(2)})});
	auto sql = BuildInsertSQL(TABLE, {"m"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (m) VALUES (MAP {'a': 1, 'b': 2});");
}

TEST_CASE("Insert SQL builder - empty MAP", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	DataChunk chunk;
	InitChunk(chunk, {map_type}, {Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {}, {})});
	auto sql = BuildInsertSQL(TABLE, {"m"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (m) VALUES (MAP {});");
}

TEST_CASE("Insert SQL builder - NULL MAP", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	DataChunk chunk;
	InitChunk(chunk, {map_type}, {Value(map_type)});
	auto sql = BuildInsertSQL(TABLE, {"m"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (m) VALUES (NULL);");
}

TEST_CASE("Insert SQL builder - MAP with NULL value", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	DataChunk chunk;
	InitChunk(chunk, {map_type},
	          {Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {Value("x")}, {Value(LogicalType::INTEGER)})});
	auto sql = BuildInsertSQL(TABLE, {"m"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (m) VALUES (MAP {'x': NULL});");
}

TEST_CASE("Insert SQL builder - MAP with integer keys", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::VARCHAR);
	DataChunk chunk;
	InitChunk(chunk, {map_type},
	          {Value::MAP(LogicalType::INTEGER, LogicalType::VARCHAR, {Value::INTEGER(1), Value::INTEGER(2)},
	                      {Value("one"), Value("two")})});
	auto sql = BuildInsertSQL(TABLE, {"m"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (m) VALUES (MAP {1: 'one', 2: 'two'});");
}

TEST_CASE("Insert SQL builder - MAP inside STRUCT", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	auto struct_type = LogicalType::STRUCT({{"label", LogicalType::VARCHAR}, {"props", map_type}});
	DataChunk chunk;
	InitChunk(chunk, {struct_type},
	          {Value::STRUCT({{"label", Value("item1")},
	                          {"props", Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {Value("weight")},
	                                               {Value::INTEGER(10)})}})});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (s) VALUES ({'label': 'item1', 'props': MAP {'weight': 10}});");
}

TEST_CASE("Insert SQL builder - LIST of STRUCTs", "[duckhog][insert-sql]") {
	auto struct_type = LogicalType::STRUCT({{"name", LogicalType::VARCHAR}, {"qty", LogicalType::INTEGER}});
	auto list_type = LogicalType::LIST(struct_type);
	DataChunk chunk;
	InitChunk(chunk, {list_type},
	          {Value::LIST(struct_type, {Value::STRUCT({{"name", Value("apple")}, {"qty", Value::INTEGER(5)}}),
	                                     Value::STRUCT({{"name", Value("banana")}, {"qty", Value::INTEGER(3)}})})});
	auto sql = BuildInsertSQL(TABLE, {"items"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (items) VALUES ([{'name': 'apple', 'qty': 5}, {'name': 'banana', "
	               "'qty': 3}]);");
}

// ============================================================
// Nested type edge cases — SQL injection / quoting
// ============================================================

TEST_CASE("Insert SQL builder - MAP with single-quote in VARCHAR key", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	DataChunk chunk;
	InitChunk(chunk, {map_type},
	          {Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {Value("it's")}, {Value::INTEGER(1)})});
	auto sql = BuildInsertSQL(TABLE, {"m"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (m) VALUES (MAP {'it''s': 1});");
}

TEST_CASE("Insert SQL builder - STRUCT with single-quote in VARCHAR value", "[duckhog][insert-sql]") {
	auto struct_type = LogicalType::STRUCT({{"name", LogicalType::VARCHAR}});
	DataChunk chunk;
	InitChunk(chunk, {struct_type}, {Value::STRUCT({{"name", Value("O'Reilly")}})});
	auto sql = BuildInsertSQL(TABLE, {"s"}, chunk);
	REQUIRE(sql == "INSERT INTO ducklake.myschema.t (s) VALUES ({'name': 'O''Reilly'});");
}

TEST_CASE("Insert SQL builder - MAP with SQL injection in key", "[duckhog][insert-sql]") {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	DataChunk chunk;
	InitChunk(
	    chunk, {map_type},
	    {Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {Value("'; DROP TABLE t; --")}, {Value::INTEGER(1)})});
	auto sql = BuildInsertSQL(TABLE, {"m"}, chunk);
	// Key must be safely quoted
	REQUIRE(sql.find("'''; DROP TABLE t; --'") != string::npos);
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
