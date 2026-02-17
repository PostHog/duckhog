#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"

using namespace duckdb;
using std::string;

namespace {

const string ATTACHED = "remote_flight";
const string REMOTE = "ducklake";

// Parse an ALTER TABLE SQL string into an AlterInfo, then rewrite catalog (mimicking Alter() logic).
unique_ptr<AlterInfo> ParseAlter(const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	D_ASSERT(parser.statements.size() == 1);
	auto &alter_stmt = parser.statements[0]->Cast<AlterStatement>();
	return std::move(alter_stmt.info);
}

// Simulate the catalog rewrite that Alter() does before calling RenderAlterTableSQL / ToString().
string RewriteAndRender(const AlterInfo &info, const string &attached_catalog, const string &remote_catalog) {
	auto copied = info.Copy();
	if (StringUtil::CIEquals(copied->catalog, attached_catalog)) {
		copied->catalog = remote_catalog;
	}
	return copied->ToString();
}

} // namespace

// ============================================================
// ParseAlter helper validation
// ============================================================

TEST_CASE("Rename table - ParseAlter produces correct AST fields", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO t_new");

	REQUIRE(info->type == AlterType::ALTER_TABLE);
	REQUIRE(info->catalog == "remote_flight");
	REQUIRE(info->schema == "s");
	REQUIRE(info->name == "t");

	auto &alter_table = info->Cast<AlterTableInfo>();
	REQUIRE(alter_table.alter_table_type == AlterTableType::RENAME_TABLE);

	auto &rename = info->Cast<RenameTableInfo>();
	REQUIRE(rename.new_table_name == "t_new");
}

TEST_CASE("Rename table - ParseAlter IF EXISTS", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE IF EXISTS remote_flight.s.t RENAME TO t_new");

	REQUIRE(info->if_not_found == OnEntryNotFound::RETURN_NULL);
	REQUIRE(info->name == "t");
	REQUIRE(info->Cast<RenameTableInfo>().new_table_name == "t_new");
}

// ============================================================
// Golden output (exact string match to detect DuckDB ToString changes)
// ============================================================

TEST_CASE("Rename table - golden output simple", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql == "ALTER TABLE ducklake.s.t RENAME TO t_new;");
}

TEST_CASE("Rename table - golden output IF EXISTS", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE IF EXISTS remote_flight.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	// Note: DuckDB's RenameTableInfo::ToString() has a formatting bug (missing space before
	// the qualified name). Our production code uses a custom RenderRenameTableSQL that
	// produces correct output. This test validates ToString() behavior for regression detection.
	REQUIRE(sql == "ALTER TABLE  IF EXISTSducklake.s.t RENAME TO t_new;");
}

// ============================================================
// Catalog rewriting
// ============================================================

TEST_CASE("Rename table - catalog rewrite attached to remote", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake.s.t") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("t_new") != string::npos);
}

TEST_CASE("Rename table - already remote catalog unchanged", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE ducklake.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake.s.t") != string::npos);
	REQUIRE(sql.find("t_new") != string::npos);
}

TEST_CASE("Rename table - case insensitive catalog match", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE REMOTE_FLIGHT.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("REMOTE_FLIGHT") == string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("Rename table - no catalog passthrough", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake") == string::npos);
	REQUIRE(sql.find("s.t") != string::npos);
	REQUIRE(sql.find("t_new") != string::npos);
}

// ============================================================
// Identifier quoting
// ============================================================

TEST_CASE("Rename table - reserved word old name quoted", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.\"select\" RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"select\"") != string::npos);
	REQUIRE(sql.find("t_new") != string::npos);
}

TEST_CASE("Rename table - reserved word new name quoted", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO \"table\"");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"table\"") != string::npos);
}

TEST_CASE("Rename table - schema with spaces", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.\"my schema\".t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("my schema") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("Rename table - schema with dots", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.\"my.schema\".t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"my.schema\"") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("Rename table - both old and new are reserved words", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.\"select\" RENAME TO \"table\"");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"select\"") != string::npos);
	REQUIRE(sql.find("\"table\"") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("Rename table - unicode old name", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.\"表\" RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("表") != string::npos);
	REQUIRE(sql.find("t_new") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("Rename table - unicode new name", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO \"テーブル\"");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("テーブル") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("Rename table - unicode schema name", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.\"スキーマ\".t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("スキーマ") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("Rename table - new name with spaces", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO \"new table\"");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("new table") != string::npos);
}

// ============================================================
// Catalog rewriting - additional
// ============================================================

TEST_CASE("Rename table - bare table name no schema no catalog", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake") == string::npos);
	REQUIRE(sql.find("RENAME TO t_new") != string::npos);
}

TEST_CASE("Rename table - mixed case remote catalog unchanged", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE Remote_Flight.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("Remote_Flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

// ============================================================
// Immutability
// ============================================================

TEST_CASE("Rename table - original info unchanged after rewrite", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO t_new");

	RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(info->catalog == "remote_flight");
	REQUIRE(info->schema == "s");
	REQUIRE(info->name == "t");
	REQUIRE(info->Cast<RenameTableInfo>().new_table_name == "t_new");
}

// ============================================================
// Output format
// ============================================================

TEST_CASE("Rename table - output ends with semicolon", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.back() == ';');
}

TEST_CASE("Rename table - output contains RENAME TO", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("RENAME TO") != string::npos);
}

TEST_CASE("Rename table - output is parseable SQL", "[duckhog][alter-table][rename]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RENAME TO t_new");

	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	Parser parser;
	REQUIRE_NOTHROW(parser.ParseQuery(sql));
	REQUIRE(parser.statements.size() == 1);
	REQUIRE(parser.statements[0]->type == StatementType::ALTER_STATEMENT);
}
