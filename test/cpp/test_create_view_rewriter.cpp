#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "execution/posthog_dml_rewriter.hpp"

using namespace duckdb;
using std::string;

namespace {

const string VIEW_ATTACHED = "remote_flight";
const string VIEW_REMOTE = "ducklake";

// Parse a CREATE VIEW SQL string into a CreateViewInfo.
unique_ptr<CreateViewInfo> ParseCreateView(const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	D_ASSERT(parser.statements.size() == 1);
	auto &create_stmt = parser.statements[0]->Cast<CreateStatement>();
	return unique_ptr_cast<CreateInfo, CreateViewInfo>(std::move(create_stmt.info));
}

} // namespace

// ============================================================
// ParseCreateView helper validation
// ============================================================

TEST_CASE("View rewriter - ParseCreateView produces correct AST fields", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT i FROM remote_flight.s.t");

	REQUIRE(info->catalog == "remote_flight");
	REQUIRE(info->schema == "s");
	REQUIRE(info->view_name == "v");
	REQUIRE(info->query != nullptr);
	REQUIRE(info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT);
	REQUIRE(info->aliases.empty());
}

TEST_CASE("View rewriter - ParseCreateView with aliases", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v(a, b) AS SELECT 1, 2");

	REQUIRE(info->view_name == "v");
	REQUIRE(info->aliases.size() == 2);
	REQUIRE(info->aliases[0] == "a");
	REQUIRE(info->aliases[1] == "b");
}

TEST_CASE("View rewriter - ParseCreateView OR REPLACE sets on_conflict", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE OR REPLACE VIEW remote_flight.s.v AS SELECT 1");

	REQUIRE(info->on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT);
}

TEST_CASE("View rewriter - ParseCreateView IF NOT EXISTS sets on_conflict", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW IF NOT EXISTS remote_flight.s.v AS SELECT 1");

	REQUIRE(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
}

// ============================================================
// Golden output (exact string match to detect DuckDB ToString changes)
// ============================================================

TEST_CASE("View rewriter - golden output simple view", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT i FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql == "CREATE VIEW ducklake.s.v AS SELECT i FROM ducklake.s.t;");
}

TEST_CASE("View rewriter - golden output with aliases", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v(a, b) AS SELECT i, j FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql == "CREATE VIEW ducklake.s.v (a, b) AS SELECT i, j FROM ducklake.s.t;");
}

// ============================================================
// Basic functionality
// ============================================================

TEST_CASE("View rewriter - simple view", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT i FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("CREATE VIEW") != string::npos);
	REQUIRE(sql.find("ducklake.s.v") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - view with column aliases", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v(a, b) AS SELECT i, j FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.v") != string::npos);
	// Aliases appear after view name; exact format checked in golden test
	REQUIRE(sql.find("a, b") != string::npos);
}

// ============================================================
// Catalog rewriting - view name
// ============================================================

TEST_CASE("View rewriter - catalog rewrite in view name", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("ducklake.s.v") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - catalog already matches remote", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW ducklake.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("ducklake.s.v") != string::npos);
}

TEST_CASE("View rewriter - case insensitive catalog match", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW REMOTE_FLIGHT.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("REMOTE_FLIGHT") == string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("View rewriter - mixed case catalog", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW Remote_Flight.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("Remote_Flight") == string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("View rewriter - no catalog specified", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	// No catalog to rewrite — should pass through
	REQUIRE(sql.find("s.v") != string::npos);
	REQUIRE(sql.find("ducklake") == string::npos);
}

TEST_CASE("View rewriter - bare view name no schema", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("CREATE VIEW v ") != string::npos);
}

// ============================================================
// Catalog rewriting - SELECT query table refs
// ============================================================

TEST_CASE("View rewriter - single table with attached catalog in query", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT * FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	// Both view name and table ref should be rewritten
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.v") != string::npos);
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

TEST_CASE("View rewriter - multiple tables in query", "[duckhog][dml-rewriter][create-view]") {
	auto info =
	    ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT * FROM remote_flight.s.t1, remote_flight.s.t2");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t1") != string::npos);
	REQUIRE(sql.find("ducklake.s.t2") != string::npos);
}

TEST_CASE("View rewriter - table without catalog in query unchanged", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT * FROM s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	// View name rewritten
	REQUIRE(sql.find("ducklake.s.v") != string::npos);
	// Table ref NOT prefixed with ducklake
	REQUIRE(sql.find("ducklake.s.t") == string::npos);
	// But s.t is still present in the query body
	REQUIRE(sql.find("FROM s.t") != string::npos);
}

TEST_CASE("View rewriter - table with alias in query", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT x.i FROM remote_flight.s.t AS x");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

// ============================================================
// Catalog rewriting - SELECT query column refs
// ============================================================

TEST_CASE("View rewriter - qualified column ref rewritten", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT remote_flight.s.t.i FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	// Column ref should be rewritten to ducklake.s.t.i
	REQUIRE(sql.find("ducklake.s.t.i") != string::npos);
}

TEST_CASE("View rewriter - unqualified column ref unchanged", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT i FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	// 'i' should appear as a bare column name in SELECT
	REQUIRE(sql.find("SELECT i") != string::npos);
}

// ============================================================
// Catalog rewriting - subqueries
// ============================================================

TEST_CASE("View rewriter - subquery in FROM", "[duckhog][dml-rewriter][create-view]") {
	auto info =
	    ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT * FROM (SELECT * FROM remote_flight.s.t) AS sub");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

TEST_CASE("View rewriter - subquery in WHERE IN", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT * FROM remote_flight.s.t1 "
	                            "WHERE i IN (SELECT j FROM remote_flight.s.t2)");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t1") != string::npos);
	REQUIRE(sql.find("ducklake.s.t2") != string::npos);
}

TEST_CASE("View rewriter - scalar subquery in SELECT list", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT (SELECT max(j) FROM remote_flight.s.t2) AS mx FROM remote_flight.s.t1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t1") != string::npos);
	REQUIRE(sql.find("ducklake.s.t2") != string::npos);
}

TEST_CASE("View rewriter - correlated subquery WHERE EXISTS", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT * FROM remote_flight.s.t1 WHERE EXISTS "
	                            "(SELECT 1 FROM remote_flight.s.t2 WHERE t2.id = t1.id)");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t1") != string::npos);
	REQUIRE(sql.find("ducklake.s.t2") != string::npos);
}

TEST_CASE("View rewriter - external catalog in subquery throws", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT * FROM remote_flight.s.t1 "
	                            "WHERE i IN (SELECT j FROM other_catalog.s.t2)");

	REQUIRE_THROWS_AS(BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE), BinderException);
}

// ============================================================
// Catalog rewriting - JOINs
// ============================================================

TEST_CASE("View rewriter - INNER JOIN both tables attached", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT * FROM remote_flight.s.t1 JOIN remote_flight.s.t2 ON t1.id = t2.id");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t1") != string::npos);
	REQUIRE(sql.find("ducklake.s.t2") != string::npos);
}

TEST_CASE("View rewriter - JOIN with catalog-qualified ON condition", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT * FROM remote_flight.s.t1 JOIN remote_flight.s.t2 "
	                            "ON remote_flight.s.t1.id = remote_flight.s.t2.id");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - JOIN with subquery on one side", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT * FROM remote_flight.s.t1 "
	                            "JOIN (SELECT * FROM remote_flight.s.t2) AS sub ON t1.id = sub.id");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t1") != string::npos);
	REQUIRE(sql.find("ducklake.s.t2") != string::npos);
}

// ============================================================
// Catalog rewriting - CTEs
// ============================================================

TEST_CASE("View rewriter - CTE body references attached table", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "WITH cte AS (SELECT * FROM remote_flight.s.t) SELECT * FROM cte");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

// ============================================================
// Catalog rewriting - set operations
// ============================================================

TEST_CASE("View rewriter - UNION of two SELECTs", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT i FROM remote_flight.s.t1 UNION SELECT i FROM remote_flight.s.t2");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t1") != string::npos);
	REQUIRE(sql.find("ducklake.s.t2") != string::npos);
}

TEST_CASE("View rewriter - UNION ALL", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT i FROM remote_flight.s.t1 UNION ALL SELECT i FROM remote_flight.s.t2");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - INTERSECT", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT i FROM remote_flight.s.t1 INTERSECT SELECT i FROM remote_flight.s.t2");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - EXCEPT", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT i FROM remote_flight.s.t1 EXCEPT SELECT i FROM remote_flight.s.t2");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
}

// ============================================================
// on_conflict variations
// ============================================================

TEST_CASE("View rewriter - ERROR_ON_CONFLICT default", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("IF NOT EXISTS") == string::npos);
	REQUIRE(sql.find("OR REPLACE") == string::npos);
	REQUIRE(sql.find("CREATE VIEW") != string::npos);
}

TEST_CASE("View rewriter - IF NOT EXISTS", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW IF NOT EXISTS remote_flight.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("IF NOT EXISTS") != string::npos);
	REQUIRE(sql.find("ducklake.s.v") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - OR REPLACE", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE OR REPLACE VIEW remote_flight.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("OR REPLACE") != string::npos);
	REQUIRE(sql.find("ducklake.s.v") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - IF NOT EXISTS with query catalog rewrite", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW IF NOT EXISTS remote_flight.s.v AS SELECT * FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("IF NOT EXISTS") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

TEST_CASE("View rewriter - OR REPLACE with query catalog rewrite", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE OR REPLACE VIEW remote_flight.s.v AS SELECT * FROM remote_flight.s.t");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("OR REPLACE") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t") != string::npos);
}

// ============================================================
// Identifier quoting
// ============================================================

TEST_CASE("View rewriter - reserved word view name", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.\"select\" AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("\"select\"") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - schema with spaces", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.\"my schema\".v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("my schema") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - schema with dots", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.\"my.schema\".v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("\"my.schema\"") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

TEST_CASE("View rewriter - unicode view name", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.\"表\" AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("表") != string::npos);
	REQUIRE(sql.find("remote_flight") == string::npos);
}

TEST_CASE("View rewriter - unicode schema name", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.\"スキーマ\".v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("スキーマ") != string::npos);
	REQUIRE(sql.find("ducklake") != string::npos);
}

// ============================================================
// Error cases
// ============================================================

TEST_CASE("View rewriter - external catalog in view definition throws", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW other_catalog.s.v AS SELECT 1");

	REQUIRE_THROWS_AS(BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE), BinderException);
}

TEST_CASE("View rewriter - external catalog case insensitive throws", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW OTHER_CATALOG.s.v AS SELECT 1");

	REQUIRE_THROWS_AS(BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE), BinderException);
}

TEST_CASE("View rewriter - external catalog in query table ref throws", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT * FROM other_catalog.s.t");

	REQUIRE_THROWS_AS(BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE), BinderException);
}

TEST_CASE("View rewriter - external catalog in query column ref throws", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT other_catalog.s.t.col FROM remote_flight.s.t");

	REQUIRE_THROWS_AS(BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE), BinderException);
}

// ============================================================
// Immutability
// ============================================================

TEST_CASE("View rewriter - does not modify original catalog", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT * FROM remote_flight.s.t");

	BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(info->catalog == VIEW_ATTACHED);
	REQUIRE(info->schema == "s");
	REQUIRE(info->view_name == "v");
}

TEST_CASE("View rewriter - does not modify original aliases", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v(a, b) AS SELECT 1, 2");

	BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(info->aliases.size() == 2);
	REQUIRE(info->aliases[0] == "a");
	REQUIRE(info->aliases[1] == "b");
}

TEST_CASE("View rewriter - does not modify original on_conflict", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE OR REPLACE VIEW remote_flight.s.v AS SELECT 1");

	BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(info->on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT);
	REQUIRE(info->catalog == VIEW_ATTACHED);
}

TEST_CASE("View rewriter - does not modify original query", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT * FROM remote_flight.s.t");
	auto original_query_str = info->query->ToString();

	BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(info->query->ToString() == original_query_str);
}

// ============================================================
// Stress / complexity
// ============================================================

TEST_CASE("View rewriter - many column aliases", "[duckhog][dml-rewriter][create-view]") {
	string alias_list;
	string select_list;
	for (int i = 0; i < 50; i++) {
		if (i > 0) {
			alias_list += ", ";
			select_list += ", ";
		}
		alias_list += "c" + std::to_string(i);
		select_list += std::to_string(i);
	}
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v(" + alias_list + ") AS SELECT " + select_list);

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.v") != string::npos);
	REQUIRE(sql.find("c0") != string::npos);
	REQUIRE(sql.find("c49") != string::npos);
}

TEST_CASE("View rewriter - complex query with JOINs subqueries and catalog refs",
          "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS "
	                            "SELECT t1.i, sub.j FROM remote_flight.s.t1 "
	                            "JOIN (SELECT j FROM remote_flight.s.t2 WHERE j > 0) AS sub ON t1.i = sub.j "
	                            "WHERE t1.i IN (SELECT k FROM remote_flight.s.t3)");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find("remote_flight") == string::npos);
	REQUIRE(sql.find("ducklake.s.t1") != string::npos);
	REQUIRE(sql.find("ducklake.s.t2") != string::npos);
	REQUIRE(sql.find("ducklake.s.t3") != string::npos);
}

// ============================================================
// Output format
// ============================================================

TEST_CASE("View rewriter - output ends with semicolon", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.back() == ';');
}

TEST_CASE("View rewriter - output contains AS keyword", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT 1");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	REQUIRE(sql.find(" AS ") != string::npos);
}

TEST_CASE("View rewriter - output is parseable SQL", "[duckhog][dml-rewriter][create-view]") {
	auto info = ParseCreateView("CREATE VIEW remote_flight.s.v AS SELECT i FROM remote_flight.s.t WHERE i > 0");

	auto sql = BuildRemoteCreateViewSQL(*info, VIEW_ATTACHED, VIEW_REMOTE);

	// Output should parse successfully as a CREATE VIEW statement
	Parser parser;
	REQUIRE_NOTHROW(parser.ParseQuery(sql));
	REQUIRE(parser.statements.size() == 1);
	REQUIRE(parser.statements[0]->type == StatementType::CREATE_STATEMENT);
}
