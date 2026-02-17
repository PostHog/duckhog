#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "execution/posthog_dml_rewriter.hpp"

using namespace duckdb;
using std::string;

namespace {

// Attached catalog name (what DuckDB sees locally) and remote catalog name
// (what the Flight SQL server sees).
const string CTAS_ATTACHED = "remote_flight";
const string CTAS_REMOTE = "ducklake";

} // namespace

// Helper to build a CreateTableInfo with columns and no query (simulates post-binder state for CTAS).
static unique_ptr<CreateTableInfo> MakeCreateTableInfo(const string &catalog, const string &schema, const string &table,
                                                       vector<std::pair<string, LogicalType>> columns) {
	auto info = make_uniq<CreateTableInfo>(catalog, schema, table);
	for (auto &col : columns) {
		info->columns.AddColumn(ColumnDefinition(std::move(col.first), std::move(col.second)));
	}
	// query is nullptr — simulates the post-binding state where the binder has resolved
	// columns from the SELECT and cleared the query field.
	return info;
}

TEST_CASE("CTAS rewriter - basic two-column table", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "myschema", "new_table",
	                                {{"id", LogicalType::INTEGER}, {"name", LogicalType::VARCHAR}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("CREATE TABLE") != string::npos);
	REQUIRE(sql.find("ducklake.myschema.new_table") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("id") != string::npos);
	REQUIRE(sql.find("INTEGER") != string::npos);
	REQUIRE(sql.find("name") != string::npos);
	REQUIRE(sql.find("VARCHAR") != string::npos);
	// Should NOT contain AS SELECT (no query)
	REQUIRE(sql.find("AS") == string::npos);
}

TEST_CASE("CTAS rewriter - catalog rewrite", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::BIGINT}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("ducklake.s.t") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("CTAS rewriter - catalog already matches remote", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_REMOTE, "s", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// Should preserve the remote catalog name
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

TEST_CASE("CTAS rewriter - IF NOT EXISTS", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("IF NOT EXISTS") != string::npos);
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

TEST_CASE("CTAS rewriter - OR REPLACE", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});
	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("OR REPLACE") != string::npos);
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

TEST_CASE("CTAS rewriter - external catalog throws", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo("some_other_catalog", "s", "t", {{"x", LogicalType::INTEGER}});

	REQUIRE_THROWS_AS(BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE), BinderException);
}

TEST_CASE("CTAS rewriter - quoted identifiers with spaces", "[duckhog][dml-rewriter][ctas]") {
	auto info =
	    MakeCreateTableInfo(CTAS_ATTACHED, "weird schema", "weird table", {{"weird col", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
	REQUIRE(sql.find("weird schema") != string::npos);
	REQUIRE(sql.find("weird table") != string::npos);
	REQUIRE(sql.find("weird col") != string::npos);
}

TEST_CASE("CTAS rewriter - default schema", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "main", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// QualifierToString omits "main" schema when catalog is set, but catalog is set so it should appear
	REQUIRE(sql.find("ducklake") != string::npos);
	REQUIRE(sql.find("t") != string::npos);
}

TEST_CASE("CTAS rewriter - multiple column types", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t",
	                                {{"a", LogicalType::BOOLEAN},
	                                 {"b", LogicalType::DOUBLE},
	                                 {"c", LogicalType::DATE},
	                                 {"d", LogicalType::TIMESTAMP}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("BOOLEAN") != string::npos);
	REQUIRE(sql.find("DOUBLE") != string::npos);
	REQUIRE(sql.find("DATE") != string::npos);
	REQUIRE(sql.find("TIMESTAMP") != string::npos);
}

TEST_CASE("CTAS rewriter - case insensitive catalog match", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo("REMOTE_FLIGHT", "s", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("REMOTE_FLIGHT") == string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("CTAS rewriter - does not modify original", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});

	BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// Original should be unchanged
	REQUIRE(info->catalog == CTAS_ATTACHED);
}

// --- Catalog edge cases ---

TEST_CASE("CTAS rewriter - no catalog specified", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo("", "s", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// Empty catalog — no rewrite needed, should pass through
	REQUIRE(sql.find("s.t") != string::npos);
}

TEST_CASE("CTAS rewriter - empty catalog empty schema", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo("", "", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// Should just have the bare table name
	REQUIRE(sql.find("CREATE TABLE t(") != string::npos);
}

TEST_CASE("CTAS rewriter - bare table name no schema", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// Catalog rewritten, no schema
	REQUIRE(sql.find("ducklake") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("CTAS rewriter - mixed case catalog Remote_Flight", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo("Remote_Flight", "s", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("Remote_Flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

// --- on_conflict variations ---

TEST_CASE("CTAS rewriter - ERROR_ON_CONFLICT (default)", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});
	info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("IF NOT EXISTS") == string::npos);
	REQUIRE(sql.find("OR REPLACE") == string::npos);
	REQUIRE(sql.find("CREATE TABLE") != string::npos);
}

TEST_CASE("CTAS rewriter - IF NOT EXISTS preserves catalog rewrite", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("CREATE TABLE") != string::npos);
	REQUIRE(sql.find("IF NOT EXISTS") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("CTAS rewriter - OR REPLACE preserves catalog rewrite", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});
	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("CREATE OR REPLACE TABLE") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

// --- Output format ---

TEST_CASE("CTAS rewriter - output ends with semicolon", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.back() == ';');
}

TEST_CASE("CTAS rewriter - output contains parenthesized column list", "[duckhog][dml-rewriter][ctas]") {
	auto info =
	    MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// Should have opening and closing parens for column defs
	REQUIRE(sql.find("(") != string::npos);
	REQUIRE(sql.find(")") != string::npos);
}

TEST_CASE("CTAS rewriter - single column table", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("ducklake.s.t") != string::npos);
	REQUIRE(sql.find("x") != string::npos);
	REQUIRE(sql.find("INTEGER") != string::npos);
}

// --- Quoted identifiers ---

TEST_CASE("CTAS rewriter - reserved word column name", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t",
	                                {{"select", LogicalType::INTEGER}, {"from", LogicalType::VARCHAR}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// Reserved words should be quoted
	REQUIRE(sql.find("\"select\"") != string::npos);
	REQUIRE(sql.find("\"from\"") != string::npos);
}

TEST_CASE("CTAS rewriter - reserved word table name", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "table", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("\"table\"") != string::npos);
}

TEST_CASE("CTAS rewriter - unicode table and column names", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "表", {{"列", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
	REQUIRE(sql.find("表") != string::npos);
	REQUIRE(sql.find("列") != string::npos);
}

TEST_CASE("CTAS rewriter - schema with special characters", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "my.schema", "t", {{"x", LogicalType::INTEGER}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	// Schema with dots should be quoted
	REQUIRE(sql.find("\"my.schema\"") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

// --- Column type variety ---

TEST_CASE("CTAS rewriter - BIGINT column", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::BIGINT}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("BIGINT") != string::npos);
}

TEST_CASE("CTAS rewriter - FLOAT column", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::FLOAT}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("FLOAT") != string::npos);
}

TEST_CASE("CTAS rewriter - BLOB column", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::BLOB}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("BLOB") != string::npos);
}

TEST_CASE("CTAS rewriter - INTERVAL column", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTERVAL}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("INTERVAL") != string::npos);
}

TEST_CASE("CTAS rewriter - HUGEINT column", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::HUGEINT}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("HUGEINT") != string::npos);
}

TEST_CASE("CTAS rewriter - SMALLINT and TINYINT columns", "[duckhog][dml-rewriter][ctas]") {
	auto info =
	    MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"a", LogicalType::SMALLINT}, {"b", LogicalType::TINYINT}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("SMALLINT") != string::npos);
	REQUIRE(sql.find("TINYINT") != string::npos);
}

TEST_CASE("CTAS rewriter - LIST type column", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"tags", LogicalType::LIST(LogicalType::VARCHAR)}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("VARCHAR[]") != string::npos);
}

TEST_CASE("CTAS rewriter - MAP type column", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t",
	                                {{"props", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER)}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("MAP") != string::npos);
}

// --- External catalog variations ---

TEST_CASE("CTAS rewriter - external catalog case insensitive throws", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo("SOME_OTHER_CATALOG", "s", "t", {{"x", LogicalType::INTEGER}});

	REQUIRE_THROWS_AS(BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE), BinderException);
}

TEST_CASE("CTAS rewriter - external catalog with spaces throws", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo("other catalog", "s", "t", {{"x", LogicalType::INTEGER}});

	REQUIRE_THROWS_AS(BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE), BinderException);
}

// --- Query field handling ---

TEST_CASE("CTAS rewriter - query field is cleared in output", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});
	// Simulate a CreateTableInfo that still has a query attached (pre-binder clearing)
	// The rewriter should clear it and produce pure DDL
	// We can't easily construct a SelectStatement here, but we verify the output has no AS SELECT
	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find(" AS ") == string::npos);
	REQUIRE(sql.find("SELECT") == string::npos);
}

// --- Stress ---

TEST_CASE("CTAS rewriter - many columns", "[duckhog][dml-rewriter][ctas]") {
	vector<std::pair<string, LogicalType>> cols;
	for (int i = 0; i < 50; i++) {
		cols.push_back({"c" + std::to_string(i), LogicalType::INTEGER});
	}
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", std::move(cols));

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
	REQUIRE(sql.find("c0") != string::npos);
	REQUIRE(sql.find("c49") != string::npos);
}

TEST_CASE("CTAS rewriter - columns with all basic types", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t",
	                                {{"a", LogicalType::BOOLEAN},
	                                 {"b", LogicalType::TINYINT},
	                                 {"c", LogicalType::SMALLINT},
	                                 {"d", LogicalType::INTEGER},
	                                 {"e", LogicalType::BIGINT},
	                                 {"f", LogicalType::FLOAT},
	                                 {"g", LogicalType::DOUBLE},
	                                 {"h", LogicalType::VARCHAR},
	                                 {"i", LogicalType::BLOB},
	                                 {"j", LogicalType::DATE},
	                                 {"k", LogicalType::TIMESTAMP},
	                                 {"l", LogicalType::INTERVAL}});

	auto sql = BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(sql.find("BOOLEAN") != string::npos);
	REQUIRE(sql.find("TINYINT") != string::npos);
	REQUIRE(sql.find("SMALLINT") != string::npos);
	REQUIRE(sql.find("INTEGER") != string::npos);
	REQUIRE(sql.find("BIGINT") != string::npos);
	REQUIRE(sql.find("FLOAT") != string::npos);
	REQUIRE(sql.find("DOUBLE") != string::npos);
	REQUIRE(sql.find("VARCHAR") != string::npos);
	REQUIRE(sql.find("BLOB") != string::npos);
	REQUIRE(sql.find("DATE") != string::npos);
	REQUIRE(sql.find("TIMESTAMP") != string::npos);
	REQUIRE(sql.find("INTERVAL") != string::npos);
}

// --- Immutability ---

TEST_CASE("CTAS rewriter - does not modify original columns", "[duckhog][dml-rewriter][ctas]") {
	auto info =
	    MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});

	BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(info->catalog == CTAS_ATTACHED);
	REQUIRE(info->schema == "s");
	REQUIRE(info->table == "t");
	// Column count should be preserved
	idx_t col_count = 0;
	for (auto &col : info->columns.Physical()) {
		(void)col;
		col_count++;
	}
	REQUIRE(col_count == 2);
}

TEST_CASE("CTAS rewriter - does not modify original on_conflict", "[duckhog][dml-rewriter][ctas]") {
	auto info = MakeCreateTableInfo(CTAS_ATTACHED, "s", "t", {{"x", LogicalType::INTEGER}});
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

	BuildRemoteCreateTableSQL(*info, CTAS_ATTACHED, CTAS_REMOTE);

	REQUIRE(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
	REQUIRE(info->catalog == CTAS_ATTACHED);
}
