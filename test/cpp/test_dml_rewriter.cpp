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

TEST_CASE("Delete rewriter - external catalog throws", "[duckhog][dml-rewriter][delete]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteDeleteSQL(
	        "DELETE FROM some_other_catalog.myschema.t WHERE i = 1",
	        ATTACHED, REMOTE),
	    BinderException);
}
