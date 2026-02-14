#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "execution/posthog_dml_rewriter.hpp"

using namespace duckdb;
using namespace std;

// Attached catalog name (what DuckDB sees locally) and remote catalog name
// (what the Flight SQL server sees).
static const string ATTACHED = "remote_flight";
static const string REMOTE = "ducklake";

TEST_CASE("Delete rewriter - simple WHERE clause", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i = 1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql == "DELETE FROM ducklake.myschema.t WHERE (i = 1)");
	REQUIRE_FALSE(result.has_returning_clause);
}

TEST_CASE("Delete rewriter - no WHERE clause", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql == "DELETE FROM ducklake.myschema.t");
	REQUIRE_FALSE(result.has_returning_clause);
}

TEST_CASE("Delete rewriter - RETURNING clause sets flag", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i > 5 RETURNING *",
	    ATTACHED, REMOTE);

	REQUIRE(result.has_returning_clause);
	// non_returning_sql should have RETURNING stripped
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
	// returning_sql should wrap in CTE
	REQUIRE(result.returning_sql.find("__duckhog_deleted_rows") != string::npos);
	REQUIRE(result.returning_sql.find("RETURNING *") != string::npos);
}

TEST_CASE("Delete rewriter - RETURNING specific columns", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i > 5 RETURNING i, j",
	    ATTACHED, REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
}

TEST_CASE("Delete rewriter - catalog rewrite in WHERE column refs", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE remote_flight.myschema.t.i = 1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - USING clause gets rewritten", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t USING remote_flight.myschema.other AS o WHERE t.id = o.id",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - non-DELETE statement throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL("SELECT 1", ATTACHED, REMOTE),
	    NotImplementedException);
}

TEST_CASE("Delete rewriter - multiple statements throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL(
	        "DELETE FROM remote_flight.s.t WHERE i = 1; DELETE FROM remote_flight.s.t WHERE i = 2",
	        ATTACHED, REMOTE),
	    NotImplementedException);
}

TEST_CASE("Delete rewriter - empty query throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS(
	    RewriteRemoteDeleteSQL("", ATTACHED, REMOTE));
}

TEST_CASE("Delete rewriter - catalog already matches remote is preserved", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM ducklake.myschema.t WHERE i = 1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - external catalog in table ref throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL(
	        "DELETE FROM some_other_catalog.myschema.t WHERE i = 1",
	        ATTACHED, REMOTE),
	    BinderException);
}

TEST_CASE("Delete rewriter - external catalog in column ref throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL(
	        "DELETE FROM remote_flight.myschema.t WHERE some_other_catalog.myschema.t.i = 1",
	        ATTACHED, REMOTE),
	    BinderException);
}

// --- WHERE complexity ---

TEST_CASE("Delete rewriter - subquery in WHERE", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i IN (SELECT id FROM remote_flight.myschema.other)",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - multiple WHERE conditions", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i > 1 AND j < 10",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("AND") != string::npos);
}

TEST_CASE("Delete rewriter - function call in WHERE", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE length(name) > 5",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("length") != string::npos);
}

// --- USING variations ---

TEST_CASE("Delete rewriter - multiple USING clauses", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t USING remote_flight.s.a, remote_flight.s.b WHERE t.id = a.id AND t.id = b.id",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - USING with catalog-qualified column refs in WHERE", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t USING remote_flight.s.other AS o WHERE remote_flight.s.t.id = o.id",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s.t") != string::npos);
}

// --- Catalog edge cases ---

TEST_CASE("Delete rewriter - no catalog specified", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM myschema.t WHERE i = 1",
	    ATTACHED, REMOTE);

	// No catalog to rewrite, should pass through
	REQUIRE(result.non_returning_sql.find("myschema.t") != string::npos);
}

TEST_CASE("Delete rewriter - bare table name", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM t WHERE i = 1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("t") != string::npos);
}

TEST_CASE("Delete rewriter - case insensitive catalog match", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM REMOTE_FLIGHT.myschema.t WHERE i = 1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("REMOTE_FLIGHT") == string::npos);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- RETURNING variations ---

TEST_CASE("Delete rewriter - RETURNING with expressions", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i > 5 RETURNING i + 1 AS incremented",
	    ATTACHED, REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
	REQUIRE(result.returning_sql.find("__duckhog_deleted_rows") != string::npos);
}

TEST_CASE("Delete rewriter - RETURNING with catalog-qualified column refs", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i > 5 RETURNING remote_flight.myschema.t.i",
	    ATTACHED, REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	// The returning_sql uses RETURNING * (stripped and re-wrapped)
	REQUIRE(result.returning_sql.find("RETURNING *") != string::npos);
}

// --- Trailing semicolons ---

TEST_CASE("Delete rewriter - trailing semicolon stripped", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i = 1;",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.back() != ';');
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - trailing whitespace and semicolons stripped", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.myschema.t WHERE i = 1 ;  ",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.back() != ';');
	REQUIRE(result.non_returning_sql.back() != ' ');
}

// --- CTE rewriting (known limitation) ---

TEST_CASE("Delete rewriter - CTE table refs are rewritten", "[duckhog][dml-rewriter][delete][!mayfail]") {
	auto result = RewriteRemoteDeleteSQL(
	    "WITH candidates AS (SELECT id FROM remote_flight.s.other WHERE active = false) "
	    "DELETE FROM remote_flight.s.t USING candidates WHERE t.id = candidates.id",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Malformed input ---

TEST_CASE("Delete rewriter - UPDATE statement throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL("UPDATE remote_flight.s.t SET i = 1", ATTACHED, REMOTE),
	    NotImplementedException);
}

TEST_CASE("Delete rewriter - INSERT statement throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL("INSERT INTO remote_flight.s.t VALUES (1)", ATTACHED, REMOTE),
	    NotImplementedException);
}

TEST_CASE("Delete rewriter - semicolon-separated destructive statement throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL("DELETE FROM remote_flight.s.t WHERE i = 1; DROP TABLE remote_flight.s.t", ATTACHED, REMOTE),
	    NotImplementedException);
}

// --- Quoted identifiers ---

TEST_CASE("Delete rewriter - quoted identifiers with spaces", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.\"weird schema\".\"weird table\" WHERE \"weird col\" = 1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("weird schema") != string::npos);
	REQUIRE(result.non_returning_sql.find("weird table") != string::npos);
	REQUIRE(result.non_returning_sql.find("weird col") != string::npos);
}

TEST_CASE("Delete rewriter - SQL injection in string literal is rejected", "[duckhog][dml-rewriter][delete]") {
	// DuckDB's parser sees this as multiple statements — the rewriter rejects it
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL(
	        "DELETE FROM remote_flight.s.t WHERE name = ''; DROP TABLE t; --'",
	        ATTACHED, REMOTE),
	    NotImplementedException);
}

// --- Unicode ---

TEST_CASE("Delete rewriter - unicode table and column names", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.\"表\" WHERE \"列\" = 1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Nested and correlated subqueries ---

TEST_CASE("Delete rewriter - deeply nested subqueries", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t WHERE i IN "
	    "(SELECT id FROM remote_flight.s.t2 WHERE j IN "
	    "(SELECT k FROM remote_flight.s.t3))",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - EXISTS subquery with catalog refs", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t WHERE EXISTS "
	    "(SELECT 1 FROM remote_flight.s.other WHERE other.id = t.id)",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - NOT EXISTS subquery", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t WHERE NOT EXISTS "
	    "(SELECT 1 FROM remote_flight.s.other WHERE other.id = t.id)",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Delete rewriter - scalar subquery in WHERE", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t WHERE i > (SELECT AVG(j) FROM remote_flight.s.other)",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Combined features ---

TEST_CASE("Delete rewriter - USING combined with RETURNING", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t USING remote_flight.s.other WHERE t.id = other.id RETURNING *",
	    ATTACHED, REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
	REQUIRE(result.returning_sql.find("__duckhog_deleted_rows") != string::npos);
}

// --- Aliased target table ---

TEST_CASE("Delete rewriter - aliased target table", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t AS x WHERE x.i = 1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- WHERE expression variety ---

TEST_CASE("Delete rewriter - IS NULL in WHERE", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t WHERE i IS NULL",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("IS NULL") != string::npos);
}

TEST_CASE("Delete rewriter - BETWEEN in WHERE", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t WHERE i BETWEEN 1 AND 10",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
}

TEST_CASE("Delete rewriter - CASE expression in WHERE", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE FROM remote_flight.s.t WHERE CASE WHEN i > 5 THEN true ELSE false END",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("CASE") != string::npos);
}

// --- Whitespace ---

TEST_CASE("Delete rewriter - excessive whitespace is normalized", "[duckhog][dml-rewriter][delete]") {
	auto result = RewriteRemoteDeleteSQL(
	    "DELETE   FROM   remote_flight.s.t   WHERE   i=1",
	    ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Stress ---

TEST_CASE("Delete rewriter - many AND conditions", "[duckhog][dml-rewriter][delete]") {
	string sql = "DELETE FROM remote_flight.s.t WHERE ";
	for (int i = 0; i < 50; i++) {
		if (i > 0) {
			sql += " AND ";
		}
		sql += "c" + to_string(i) + " = " + to_string(i);
	}

	auto result = RewriteRemoteDeleteSQL(sql, ATTACHED, REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("c49") != string::npos);
}
