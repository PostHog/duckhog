#include "catch.hpp"
#include "catalog/posthog_table_entry.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

using namespace duckdb;
using std::string;

TEST_CASE("RenderAtClauseSQL - integer VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::INTEGER(1));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 1)");
}

TEST_CASE("RenderAtClauseSQL - large integer VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::BIGINT(999999));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 999999)");
}

TEST_CASE("RenderAtClauseSQL - zero VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::INTEGER(0));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 0)");
}

TEST_CASE("RenderAtClauseSQL - TIMESTAMP string", "[duckhog][at-clause]") {
	BoundAtClause clause("TIMESTAMP", Value("2024-01-15 10:30:00"));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (TIMESTAMP => '2024-01-15 10:30:00')");
}

TEST_CASE("RenderAtClauseSQL - SMALLINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::SMALLINT(42));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 42)");
}

TEST_CASE("RenderAtClauseSQL - TINYINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::TINYINT(7));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 7)");
}

TEST_CASE("RenderAtClauseSQL - unsigned UBIGINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::UBIGINT(123456789));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 123456789)");
}

TEST_CASE("RenderAtClauseSQL - DATE value is quoted", "[duckhog][at-clause]") {
	BoundAtClause clause("TIMESTAMP", Value("2024-06-01"));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (TIMESTAMP => '2024-06-01')");
}
