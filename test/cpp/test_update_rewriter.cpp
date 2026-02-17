#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "execution/posthog_dml_rewriter.hpp"

using namespace duckdb;
using std::string;

namespace {

// Attached catalog name (what DuckDB sees locally) and remote catalog name
// (what the Flight SQL server sees).
const string UPDATE_ATTACHED = "remote_flight";
const string UPDATE_REMOTE = "ducklake";

} // namespace

// --- Basic SET/WHERE ---

TEST_CASE("Update rewriter - simple SET + WHERE", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.myschema.t SET i = 1 WHERE j = 2", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("ducklake.myschema.t") != string::npos);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("SET") != string::npos);
	REQUIRE(result.non_returning_sql.find("WHERE") != string::npos);
	REQUIRE_FALSE(result.has_returning_clause);
}

TEST_CASE("Update rewriter - SET without WHERE", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 0", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("ducklake.s.t") != string::npos);
	REQUIRE(result.non_returning_sql.find("WHERE") == string::npos);
}

TEST_CASE("Update rewriter - multiple SET columns", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1, j = 2, k = 3 WHERE id = 10",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("ducklake.s.t") != string::npos);
	REQUIRE(result.non_returning_sql.find("i =") != string::npos);
	REQUIRE(result.non_returning_sql.find("j =") != string::npos);
	REQUIRE(result.non_returning_sql.find("k =") != string::npos);
}

TEST_CASE("Update rewriter - SET with expression", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = i + 1 WHERE j > 5", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("ducklake.s.t") != string::npos);
	REQUIRE(result.non_returning_sql.find("i + 1") != string::npos);
}

// --- RETURNING ---

TEST_CASE("Update rewriter - RETURNING clause sets flag", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1 WHERE j > 5 RETURNING *", UPDATE_ATTACHED,
	                                     UPDATE_REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
	REQUIRE(result.returning_sql.find("__duckhog_updated_rows") != string::npos);
	REQUIRE(result.returning_sql.find("RETURNING *") != string::npos);
}

TEST_CASE("Update rewriter - RETURNING specific columns", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1 WHERE j > 5 RETURNING i, j",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
}

TEST_CASE("Update rewriter - RETURNING with expressions", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1 WHERE j > 5 RETURNING i + 1 AS incremented",
	                           UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
	REQUIRE(result.returning_sql.find("__duckhog_updated_rows") != string::npos);
}

TEST_CASE("Update rewriter - RETURNING with catalog-qualified column refs", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1 WHERE j > 5 RETURNING remote_flight.s.t.i",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.returning_sql.find("RETURNING *") != string::npos);
}

// --- Catalog rewriting ---

TEST_CASE("Update rewriter - catalog rewrite in table ref", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("ducklake.s.t") != string::npos);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
}

TEST_CASE("Update rewriter - catalog rewrite in WHERE column refs", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1 WHERE remote_flight.s.t.j = 2",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - catalog rewrite in SET expression", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = remote_flight.s.t.j + 1", UPDATE_ATTACHED,
	                                     UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - catalog already matches remote", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE ducklake.s.t SET i = 1 WHERE j = 2", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - case insensitive catalog match", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE REMOTE_FLIGHT.s.t SET i = 1 WHERE j = 2", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("REMOTE_FLIGHT") == string::npos);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - no catalog specified", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE myschema.t SET i = 1 WHERE j = 2", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("myschema.t") != string::npos);
}

TEST_CASE("Update rewriter - bare table name", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE t SET i = 1 WHERE j = 2", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("t") != string::npos);
}

TEST_CASE("Update rewriter - external catalog in table ref throws", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS_AS(RewriteRemoteUpdateSQL("UPDATE some_other_catalog.s.t SET i = 1", UPDATE_ATTACHED, UPDATE_REMOTE),
	                  BinderException);
}

TEST_CASE("Update rewriter - external catalog in column ref throws", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS_AS(RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1 WHERE some_other_catalog.s.t.j = 2",
	                                         UPDATE_ATTACHED, UPDATE_REMOTE),
	                  BinderException);
}

// --- FROM clause ---

TEST_CASE("Update rewriter - FROM clause gets rewritten", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL(
	    "UPDATE remote_flight.s.t SET i = o.val FROM remote_flight.s.other AS o WHERE t.id = o.id", UPDATE_ATTACHED,
	    UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - FROM with catalog-qualified column refs in WHERE", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL(
	    "UPDATE remote_flight.s.t SET i = 1 FROM remote_flight.s.other AS o WHERE remote_flight.s.t.id = o.id",
	    UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s.t") != string::npos);
}

// --- Subqueries ---

TEST_CASE("Update rewriter - subquery in WHERE", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 0 WHERE j IN (SELECT id FROM remote_flight.s.other)",
	                           UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - subquery in SET expression", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL(
	    "UPDATE remote_flight.s.t SET i = (SELECT max(j) FROM remote_flight.s.other) WHERE id = 1", UPDATE_ATTACHED,
	    UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - deeply nested subqueries", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 0 WHERE j IN "
	                                     "(SELECT id FROM remote_flight.s.t2 WHERE k IN "
	                                     "(SELECT m FROM remote_flight.s.t3))",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - EXISTS subquery with catalog refs", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 0 WHERE EXISTS "
	                                     "(SELECT 1 FROM remote_flight.s.other WHERE other.id = t.id)",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - NOT EXISTS subquery", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 0 WHERE NOT EXISTS "
	                                     "(SELECT 1 FROM remote_flight.s.other WHERE other.id = t.id)",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - scalar subquery in WHERE", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL(
	    "UPDATE remote_flight.s.t SET i = 0 WHERE j > (SELECT AVG(k) FROM remote_flight.s.other)", UPDATE_ATTACHED,
	    UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Error handling ---

TEST_CASE("Update rewriter - non-UPDATE statement throws", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS_AS(RewriteRemoteUpdateSQL("SELECT 1", UPDATE_ATTACHED, UPDATE_REMOTE), NotImplementedException);
}

TEST_CASE("Update rewriter - multiple statements throws", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS_AS(RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1; UPDATE remote_flight.s.t SET j = 2",
	                                         UPDATE_ATTACHED, UPDATE_REMOTE),
	                  NotImplementedException);
}

TEST_CASE("Update rewriter - empty query throws", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS(RewriteRemoteUpdateSQL("", UPDATE_ATTACHED, UPDATE_REMOTE));
}

TEST_CASE("Update rewriter - DELETE statement throws", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteUpdateSQL("DELETE FROM remote_flight.s.t WHERE i = 1", UPDATE_ATTACHED, UPDATE_REMOTE),
	    NotImplementedException);
}

TEST_CASE("Update rewriter - INSERT statement throws", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteUpdateSQL("INSERT INTO remote_flight.s.t VALUES (1)", UPDATE_ATTACHED, UPDATE_REMOTE),
	    NotImplementedException);
}

TEST_CASE("Update rewriter - semicolon-separated destructive statement throws", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS_AS(RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1; DROP TABLE remote_flight.s.t",
	                                         UPDATE_ATTACHED, UPDATE_REMOTE),
	                  NotImplementedException);
}

// --- Formatting ---

TEST_CASE("Update rewriter - trailing semicolon stripped", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1 WHERE j = 2;", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.back() != ';');
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - trailing whitespace and semicolons stripped", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 1 WHERE j = 2 ;  ", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.back() != ';');
	REQUIRE(result.non_returning_sql.back() != ' ');
}

TEST_CASE("Update rewriter - excessive whitespace is normalized", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE   remote_flight.s.t   SET   i=1   WHERE   j=2", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Quoted identifiers ---

TEST_CASE("Update rewriter - quoted identifiers with spaces", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.\"weird schema\".\"weird table\" SET \"weird col\" = 1",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("weird schema") != string::npos);
	REQUIRE(result.non_returning_sql.find("weird table") != string::npos);
	REQUIRE(result.non_returning_sql.find("weird col") != string::npos);
}

TEST_CASE("Update rewriter - unicode table and column names", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.\"表\" SET \"列\" = 1", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("表") != string::npos);
	REQUIRE(result.non_returning_sql.find("列") != string::npos);
}

TEST_CASE("Update rewriter - SQL injection in string literal is rejected", "[duckhog][dml-rewriter][update]") {
	REQUIRE_THROWS_AS(RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET name = ''; DROP TABLE t; --'",
	                                         UPDATE_ATTACHED, UPDATE_REMOTE),
	                  NotImplementedException);
}

// --- Edge cases ---

TEST_CASE("Update rewriter - aliased target table", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t AS x SET i = 1 WHERE x.j = 2", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - CTE table refs are rewritten", "[duckhog][dml-rewriter][update][!mayfail]") {
	auto result = RewriteRemoteUpdateSQL("WITH vals AS (SELECT id, 99 AS new_val FROM remote_flight.s.other) "
	                                     "UPDATE remote_flight.s.t SET i = vals.new_val FROM vals WHERE t.id = vals.id",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Update rewriter - IS NULL in WHERE", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 0 WHERE j IS NULL", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("IS NULL") != string::npos);
}

TEST_CASE("Update rewriter - BETWEEN in WHERE", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = 0 WHERE j BETWEEN 1 AND 10", UPDATE_ATTACHED,
	                                     UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
}

TEST_CASE("Update rewriter - CASE expression in SET", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET i = CASE WHEN j > 5 THEN 1 ELSE 0 END",
	                                     UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("CASE") != string::npos);
}

TEST_CASE("Update rewriter - function call in SET", "[duckhog][dml-rewriter][update]") {
	auto result =
	    RewriteRemoteUpdateSQL("UPDATE remote_flight.s.t SET name = upper(name)", UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("upper") != string::npos);
}

TEST_CASE("Update rewriter - FROM combined with RETURNING", "[duckhog][dml-rewriter][update]") {
	auto result = RewriteRemoteUpdateSQL(
	    "UPDATE remote_flight.s.t SET i = o.val FROM remote_flight.s.other AS o WHERE t.id = o.id RETURNING *",
	    UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
	REQUIRE(result.returning_sql.find("__duckhog_updated_rows") != string::npos);
}

// --- Stress ---

TEST_CASE("Update rewriter - many SET columns", "[duckhog][dml-rewriter][update]") {
	string sql = "UPDATE remote_flight.s.t SET ";
	for (int i = 0; i < 50; i++) {
		if (i > 0) {
			sql += ", ";
		}
		sql += "c" + std::to_string(i) + " = " + std::to_string(i);
	}
	sql += " WHERE id = 1";

	auto result = RewriteRemoteUpdateSQL(sql, UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("c49") != string::npos);
}

TEST_CASE("Update rewriter - many AND conditions in WHERE", "[duckhog][dml-rewriter][update]") {
	string sql = "UPDATE remote_flight.s.t SET x = 0 WHERE ";
	for (int i = 0; i < 50; i++) {
		if (i > 0) {
			sql += " AND ";
		}
		sql += "c" + std::to_string(i) + " = " + std::to_string(i);
	}

	auto result = RewriteRemoteUpdateSQL(sql, UPDATE_ATTACHED, UPDATE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("c49") != string::npos);
}
