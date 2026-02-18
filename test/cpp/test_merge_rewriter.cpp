#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "execution/posthog_dml_rewriter.hpp"

using namespace duckdb;
using std::string;

namespace {

const string MERGE_ATTACHED = "remote_flight";
const string MERGE_REMOTE = "ducklake";

} // namespace

// --- Basic MERGE ---

TEST_CASE("Merge rewriter - basic WHEN MATCHED + WHEN NOT MATCHED", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = s.val "
	                          "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s.tgt") != string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s.src") != string::npos);
	REQUIRE_FALSE(result.has_returning_clause);
}

TEST_CASE("Merge rewriter - WHEN MATCHED only", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = s.val",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("UPDATE") != string::npos);
}

TEST_CASE("Merge rewriter - WHEN NOT MATCHED only", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("INSERT") != string::npos);
}

TEST_CASE("Merge rewriter - WHEN MATCHED THEN DELETE", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN DELETE",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("DELETE") != string::npos);
}

// --- AND conditions ---

TEST_CASE("Merge rewriter - WHEN MATCHED AND condition", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED AND s.val IS NOT NULL THEN UPDATE SET val = s.val "
	                          "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Source variations ---

TEST_CASE("Merge rewriter - source in different schema", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s1.tgt AS t USING remote_flight.s2.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = s.val",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s1.tgt") != string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s2.src") != string::npos);
}

TEST_CASE("Merge rewriter - cross-catalog source throws", "[duckhog][dml-rewriter][merge]") {
	REQUIRE_THROWS_AS(
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING other_catalog.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = s.val",
	                          MERGE_ATTACHED, MERGE_REMOTE),
	    BinderException);
}

TEST_CASE("Merge rewriter - subquery as source", "[duckhog][dml-rewriter][merge]") {
	auto result = RewriteRemoteMergeSQL(
	    "MERGE INTO remote_flight.s.tgt AS t USING (SELECT id, val FROM remote_flight.s.src WHERE active) AS s "
	    "ON t.id = s.id "
	    "WHEN MATCHED THEN UPDATE SET val = s.val",
	    MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Catalog edge cases ---

TEST_CASE("Merge rewriter - catalog already matches remote", "[duckhog][dml-rewriter][merge]") {
	auto result = RewriteRemoteMergeSQL("MERGE INTO ducklake.s.tgt AS t USING ducklake.s.src AS s ON t.id = s.id "
	                                    "WHEN MATCHED THEN UPDATE SET val = s.val",
	                                    MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("ducklake.s.tgt") != string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s.src") != string::npos);
}

TEST_CASE("Merge rewriter - case insensitive catalog match", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO REMOTE_FLIGHT.s.tgt AS t USING REMOTE_FLIGHT.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = s.val",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("REMOTE_FLIGHT") == string::npos);
	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

TEST_CASE("Merge rewriter - no catalog specified", "[duckhog][dml-rewriter][merge]") {
	auto result = RewriteRemoteMergeSQL("MERGE INTO s.tgt AS t USING s.src AS s ON t.id = s.id "
	                                    "WHEN MATCHED THEN UPDATE SET val = s.val",
	                                    MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("s.tgt") != string::npos);
}

// --- RETURNING ---

TEST_CASE("Merge rewriter - RETURNING clause sets flag", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = s.val "
	                          "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val) RETURNING *",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.has_returning_clause);
	REQUIRE(result.non_returning_sql.find("RETURNING") == string::npos);
	REQUIRE(result.returning_sql.find("__duckhog_merged_rows") != string::npos);
	REQUIRE(result.returning_sql.find("RETURNING *") != string::npos);
}

// --- Error cases ---

TEST_CASE("Merge rewriter - non-MERGE statement throws", "[duckhog][dml-rewriter][merge]") {
	REQUIRE_THROWS_AS(RewriteRemoteMergeSQL("SELECT 1", MERGE_ATTACHED, MERGE_REMOTE), NotImplementedException);
}

TEST_CASE("Merge rewriter - UPDATE statement throws", "[duckhog][dml-rewriter][merge]") {
	REQUIRE_THROWS_AS(RewriteRemoteMergeSQL("UPDATE remote_flight.s.t SET i = 1", MERGE_ATTACHED, MERGE_REMOTE),
	                  NotImplementedException);
}

TEST_CASE("Merge rewriter - empty query throws", "[duckhog][dml-rewriter][merge]") {
	REQUIRE_THROWS(RewriteRemoteMergeSQL("", MERGE_ATTACHED, MERGE_REMOTE));
}

// --- Trailing semicolons ---

TEST_CASE("Merge rewriter - trailing semicolon stripped", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = s.val;",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.back() != ';');
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- Quoted identifiers ---

TEST_CASE("Merge rewriter - quoted identifiers", "[duckhog][dml-rewriter][merge]") {
	auto result = RewriteRemoteMergeSQL("MERGE INTO remote_flight.\"my schema\".\"my target\" AS t "
	                                    "USING remote_flight.\"my schema\".\"my source\" AS s ON t.id = s.id "
	                                    "WHEN MATCHED THEN UPDATE SET val = s.val",
	                                    MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("my schema") != string::npos);
	REQUIRE(result.non_returning_sql.find("my target") != string::npos);
	REQUIRE(result.non_returning_sql.find("my source") != string::npos);
}

// --- CTE as source ---

TEST_CASE("Merge rewriter - CTE as source", "[duckhog][dml-rewriter][merge]") {
	auto result = RewriteRemoteMergeSQL(
	    "MERGE INTO remote_flight.s.tgt AS t "
	    "USING (WITH cte AS (SELECT id, val FROM remote_flight.s.src) SELECT * FROM cte) AS s ON t.id = s.id "
	    "WHEN MATCHED THEN UPDATE SET val = s.val",
	    MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("cte") != string::npos);
}

// --- WHEN NOT MATCHED BY SOURCE ---

TEST_CASE("Merge rewriter - WHEN NOT MATCHED BY SOURCE rewrites", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = s.val "
	                          "WHEN NOT MATCHED BY SOURCE THEN DELETE",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("DELETE") != string::npos);
}

// --- DO NOTHING ---

TEST_CASE("Merge rewriter - DO NOTHING action", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN DO NOTHING "
	                          "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("DO NOTHING") != string::npos);
}

// --- Expression rewriting in SET clause ---

TEST_CASE("Merge rewriter - catalog ref in SET expression", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET val = remote_flight.s.src.val + 1",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
}

// --- JOIN condition with catalog-qualified refs ---

TEST_CASE("Merge rewriter - catalog-qualified columns in ON clause", "[duckhog][dml-rewriter][merge]") {
	auto result = RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s "
	                                    "ON remote_flight.s.tgt.id = remote_flight.s.src.id "
	                                    "WHEN MATCHED THEN UPDATE SET val = s.val",
	                                    MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s.tgt.id") != string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake.s.src.id") != string::npos);
}

// --- Multiple actions ---

TEST_CASE("Merge rewriter - multiple WHEN MATCHED actions", "[duckhog][dml-rewriter][merge]") {
	auto result =
	    RewriteRemoteMergeSQL("MERGE INTO remote_flight.s.tgt AS t USING remote_flight.s.src AS s ON t.id = s.id "
	                          "WHEN MATCHED AND s.active THEN UPDATE SET val = s.val "
	                          "WHEN MATCHED AND NOT s.active THEN DELETE "
	                          "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)",
	                          MERGE_ATTACHED, MERGE_REMOTE);

	REQUIRE(result.non_returning_sql.find("remote_flight") == string::npos);
	REQUIRE(result.non_returning_sql.find("ducklake") != string::npos);
	REQUIRE(result.non_returning_sql.find("UPDATE") != string::npos);
	REQUIRE(result.non_returning_sql.find("DELETE") != string::npos);
	REQUIRE(result.non_returning_sql.find("INSERT") != string::npos);
}
