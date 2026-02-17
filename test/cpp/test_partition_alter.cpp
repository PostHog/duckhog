#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"

using namespace duckdb;
using std::string;

namespace partition_alter_test {

const string ATTACHED = "remote_flight";
const string REMOTE = "ducklake";

unique_ptr<AlterInfo> ParseAlter(const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	D_ASSERT(parser.statements.size() == 1);
	auto &alter_stmt = parser.statements[0]->Cast<AlterStatement>();
	return std::move(alter_stmt.info);
}

string RewriteAndRender(const AlterInfo &info, const string &attached_catalog, const string &remote_catalog) {
	auto copied = info.Copy();
	if (StringUtil::CIEquals(copied->catalog, attached_catalog)) {
		copied->catalog = remote_catalog;
	}
	return copied->ToString();
}

// ============================================================
// ParseAlter - SET PARTITIONED BY
// ============================================================

TEST_CASE("Partition alter - ParseAlter SET PARTITIONED BY single column", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (col)");

	REQUIRE(info->type == AlterType::ALTER_TABLE);
	REQUIRE(info->catalog == "remote_flight");
	REQUIRE(info->schema == "s");
	REQUIRE(info->name == "t");

	auto &alter_table = info->Cast<AlterTableInfo>();
	REQUIRE(alter_table.alter_table_type == AlterTableType::SET_PARTITIONED_BY);

	auto &part_info = info->Cast<SetPartitionedByInfo>();
	REQUIRE(part_info.partition_keys.size() == 1);
	REQUIRE(part_info.partition_keys[0]->ToString() == "col");
}

TEST_CASE("Partition alter - ParseAlter SET PARTITIONED BY multiple columns", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (a, b, c)");

	auto &part_info = info->Cast<SetPartitionedByInfo>();
	REQUIRE(part_info.partition_keys.size() == 3);
	REQUIRE(part_info.partition_keys[0]->ToString() == "a");
	REQUIRE(part_info.partition_keys[1]->ToString() == "b");
	REQUIRE(part_info.partition_keys[2]->ToString() == "c");
}

TEST_CASE("Partition alter - ParseAlter RESET PARTITIONED BY", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RESET PARTITIONED BY");

	REQUIRE(info->type == AlterType::ALTER_TABLE);
	auto &alter_table = info->Cast<AlterTableInfo>();
	REQUIRE(alter_table.alter_table_type == AlterTableType::SET_PARTITIONED_BY);

	auto &part_info = info->Cast<SetPartitionedByInfo>();
	REQUIRE(part_info.partition_keys.empty());
}

// ============================================================
// Golden output - SET PARTITIONED BY
// ============================================================

TEST_CASE("Partition alter - golden output single column", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);
	REQUIRE(sql == "ALTER TABLE ducklake.s.t SET PARTITIONED BY (col)");
}

TEST_CASE("Partition alter - golden output multiple columns", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (a, b, c)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);
	REQUIRE(sql == "ALTER TABLE ducklake.s.t SET PARTITIONED BY (a, b, c)");
}

TEST_CASE("Partition alter - golden output RESET", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RESET PARTITIONED BY");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);
	REQUIRE(sql == "ALTER TABLE ducklake.s.t RESET PARTITIONED BY");
}

// ============================================================
// Catalog rewriting
// ============================================================

TEST_CASE("Partition alter - catalog rewrite attached to remote", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake.s.t") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("Partition alter - already remote catalog unchanged", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE ducklake.s.t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

TEST_CASE("Partition alter - case insensitive catalog match", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE REMOTE_FLIGHT.s.t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("REMOTE_FLIGHT") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("Partition alter - no catalog passthrough", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE s.t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake") == string::npos);
	REQUIRE(sql.find("s.t") != string::npos);
}

TEST_CASE("Partition alter - bare table no schema no catalog", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake") == string::npos);
	REQUIRE(sql.find("SET PARTITIONED BY (col)") != string::npos);
}

TEST_CASE("Partition alter - RESET catalog rewrite", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RESET PARTITIONED BY");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("ducklake.s.t") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("RESET PARTITIONED BY") != string::npos);
}

// ============================================================
// Identifier quoting - partition column names
// ============================================================

TEST_CASE("Partition alter - reserved word partition column", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (\"select\")");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"select\"") != string::npos);
}

TEST_CASE("Partition alter - partition column with spaces", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (\"my col\")");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"my col\"") != string::npos);
}

TEST_CASE("Partition alter - unicode partition column", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (\"列\")");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("列") != string::npos);
}

TEST_CASE("Partition alter - mixed quoted and unquoted partition columns", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (a, \"select\", c)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"select\"") != string::npos);
	auto pos_a = sql.find("(a,");
	auto pos_sel = sql.find("\"select\"");
	auto pos_c = sql.find(", c)");
	REQUIRE(pos_a != string::npos);
	REQUIRE(pos_sel != string::npos);
	REQUIRE(pos_c != string::npos);
	REQUIRE(pos_a < pos_sel);
	REQUIRE(pos_sel < pos_c);
}

// ============================================================
// Identifier quoting - table/schema names
// ============================================================

TEST_CASE("Partition alter - reserved word table name", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.\"table\" SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"table\"") != string::npos);
	REQUIRE(sql.find("SET PARTITIONED BY (col)") != string::npos);
}

TEST_CASE("Partition alter - schema with spaces", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.\"my schema\".t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("my schema") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("Partition alter - schema with dots", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.\"my.schema\".t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.find("\"my.schema\"") != string::npos);
}

// ============================================================
// Partition key ordering preserved
// ============================================================

TEST_CASE("Partition alter - key ordering preserved in output", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (z, a, m)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	auto pos_z = sql.find("z,");
	auto pos_a = sql.find(" a,");
	auto pos_m = sql.find(" m)");
	REQUIRE(pos_z != string::npos);
	REQUIRE(pos_a != string::npos);
	REQUIRE(pos_m != string::npos);
	REQUIRE(pos_z < pos_a);
	REQUIRE(pos_a < pos_m);
}

// ============================================================
// Immutability
// ============================================================

TEST_CASE("Partition alter - original info unchanged after rewrite", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (col)");

	RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(info->catalog == "remote_flight");
	REQUIRE(info->schema == "s");
	REQUIRE(info->name == "t");

	auto &part_info = info->Cast<SetPartitionedByInfo>();
	REQUIRE(part_info.partition_keys.size() == 1);
	REQUIRE(part_info.partition_keys[0]->ToString() == "col");
}

TEST_CASE("Partition alter - Copy preserves all partition keys", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (a, b, c)");

	auto copied = info->Copy();
	auto &orig = info->Cast<SetPartitionedByInfo>();
	auto &copy = copied->Cast<SetPartitionedByInfo>();

	REQUIRE(copy.partition_keys.size() == orig.partition_keys.size());
	for (idx_t i = 0; i < orig.partition_keys.size(); i++) {
		REQUIRE(copy.partition_keys[i]->ToString() == orig.partition_keys[i]->ToString());
	}
}

TEST_CASE("Partition alter - Copy of RESET preserves empty keys", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RESET PARTITIONED BY");

	auto copied = info->Copy();
	auto &copy = copied->Cast<SetPartitionedByInfo>();

	REQUIRE(copy.partition_keys.empty());
}

// ============================================================
// Output format
// ============================================================

TEST_CASE("Partition alter - SET output does not end with semicolon", "[duckhog][alter-table][partition]") {
	// Note: ToString() does NOT append semicolon (unlike our custom renderers).
	// Our production code flows through info.ToString() for partitioning, so
	// whatever semicolon behavior DuckDB uses is what gets sent to the server.
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.back() == ')');
}

TEST_CASE("Partition alter - RESET output format", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RESET PARTITIONED BY");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	REQUIRE(sql.back() == 'Y');
}

TEST_CASE("Partition alter - SET output is parseable SQL", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (col)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	Parser parser;
	REQUIRE_NOTHROW(parser.ParseQuery(sql));
	REQUIRE(parser.statements.size() == 1);
	REQUIRE(parser.statements[0]->type == StatementType::ALTER_STATEMENT);
}

TEST_CASE("Partition alter - RESET output is parseable SQL", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t RESET PARTITIONED BY");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	Parser parser;
	REQUIRE_NOTHROW(parser.ParseQuery(sql));
	REQUIRE(parser.statements.size() == 1);
	REQUIRE(parser.statements[0]->type == StatementType::ALTER_STATEMENT);
}

TEST_CASE("Partition alter - multi-column output is parseable SQL", "[duckhog][alter-table][partition]") {
	auto info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (a, b, c)");
	auto sql = RewriteAndRender(*info, ATTACHED, REMOTE);

	Parser parser;
	REQUIRE_NOTHROW(parser.ParseQuery(sql));
}

// ============================================================
// SET vs RESET distinction via ToString()
// ============================================================

TEST_CASE("Partition alter - SET and RESET use same AlterTableType", "[duckhog][alter-table][partition]") {
	auto set_info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (col)");
	auto reset_info = ParseAlter("ALTER TABLE remote_flight.s.t RESET PARTITIONED BY");

	auto &set_alter = set_info->Cast<AlterTableInfo>();
	auto &reset_alter = reset_info->Cast<AlterTableInfo>();

	// Both parse to the same enum value — distinguished only by partition_keys emptiness
	REQUIRE(set_alter.alter_table_type == AlterTableType::SET_PARTITIONED_BY);
	REQUIRE(reset_alter.alter_table_type == AlterTableType::SET_PARTITIONED_BY);

	auto &set_part = set_info->Cast<SetPartitionedByInfo>();
	auto &reset_part = reset_info->Cast<SetPartitionedByInfo>();

	REQUIRE_FALSE(set_part.partition_keys.empty());
	REQUIRE(reset_part.partition_keys.empty());
}

TEST_CASE("Partition alter - ToString distinguishes SET from RESET", "[duckhog][alter-table][partition]") {
	auto set_info = ParseAlter("ALTER TABLE remote_flight.s.t SET PARTITIONED BY (col)");
	auto reset_info = ParseAlter("ALTER TABLE remote_flight.s.t RESET PARTITIONED BY");

	auto set_sql = set_info->ToString();
	auto reset_sql = reset_info->ToString();

	REQUIRE(set_sql.find("SET PARTITIONED BY") != string::npos);
	REQUIRE(reset_sql.find("RESET PARTITIONED BY") != string::npos);
	REQUIRE(set_sql.find("RESET") == string::npos);
	// "RESET" contains "SET" as substring, so check for the full keyword instead
	REQUIRE(reset_sql.find("SET PARTITIONED BY (") == string::npos);
}

} // namespace partition_alter_test
