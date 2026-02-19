#include "catch.hpp"
#include "catalog/posthog_table_entry.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

using namespace duckdb;
using std::string;

// ============================================================
// Integer types: all should render unquoted
// ============================================================

TEST_CASE("RenderAtClauseSQL - INTEGER VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::INTEGER(1));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 1)");
}

TEST_CASE("RenderAtClauseSQL - BIGINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::BIGINT(999999));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 999999)");
}

TEST_CASE("RenderAtClauseSQL - SMALLINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::SMALLINT(42));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 42)");
}

TEST_CASE("RenderAtClauseSQL - TINYINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::TINYINT(7));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 7)");
}

TEST_CASE("RenderAtClauseSQL - UBIGINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::UBIGINT(123456789));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 123456789)");
}

TEST_CASE("RenderAtClauseSQL - UINTEGER VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::UINTEGER(50000));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 50000)");
}

TEST_CASE("RenderAtClauseSQL - USMALLINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::USMALLINT(300));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 300)");
}

TEST_CASE("RenderAtClauseSQL - UTINYINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::UTINYINT(255));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 255)");
}

TEST_CASE("RenderAtClauseSQL - HUGEINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::HUGEINT(hugeint_t(9999999)));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 9999999)");
}

TEST_CASE("RenderAtClauseSQL - UHUGEINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::UHUGEINT(uhugeint_t(1)));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 1)");
}

// ============================================================
// Boundary integer values
// ============================================================

TEST_CASE("RenderAtClauseSQL - zero VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::INTEGER(0));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 0)");
}

TEST_CASE("RenderAtClauseSQL - negative INTEGER VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::INTEGER(-1));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => -1)");
}

TEST_CASE("RenderAtClauseSQL - max BIGINT VERSION", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::BIGINT(9223372036854775807LL));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (VERSION => 9223372036854775807)");
}

// ============================================================
// Non-integer types: all should render single-quoted
// ============================================================

TEST_CASE("RenderAtClauseSQL - VARCHAR TIMESTAMP", "[duckhog][at-clause]") {
	BoundAtClause clause("TIMESTAMP", Value("2024-01-15 10:30:00"));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (TIMESTAMP => '2024-01-15 10:30:00')");
}

TEST_CASE("RenderAtClauseSQL - VARCHAR date-only", "[duckhog][at-clause]") {
	BoundAtClause clause("TIMESTAMP", Value("2024-06-01"));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (TIMESTAMP => '2024-06-01')");
}

TEST_CASE("RenderAtClauseSQL - FLOAT is quoted", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::FLOAT(1.5f));
	auto result = RenderAtClauseSQL(clause);
	// FLOAT is non-integral, so it gets quoted
	REQUIRE(result.find("'") != string::npos);
}

TEST_CASE("RenderAtClauseSQL - DOUBLE is quoted", "[duckhog][at-clause]") {
	BoundAtClause clause("VERSION", Value::DOUBLE(2.0));
	auto result = RenderAtClauseSQL(clause);
	REQUIRE(result.find("'") != string::npos);
}

TEST_CASE("RenderAtClauseSQL - BOOLEAN is quoted", "[duckhog][at-clause]") {
	BoundAtClause clause("SOMETHING", Value::BOOLEAN(true));
	auto result = RenderAtClauseSQL(clause);
	REQUIRE(result.find("'") != string::npos);
}

// ============================================================
// Single-quote escaping in non-integral values
// ============================================================

TEST_CASE("RenderAtClauseSQL - embedded single quote is escaped", "[duckhog][at-clause]") {
	BoundAtClause clause("TIMESTAMP", Value("2024-01-15'injection"));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (TIMESTAMP => '2024-01-15''injection')");
}

TEST_CASE("RenderAtClauseSQL - multiple embedded single quotes", "[duckhog][at-clause]") {
	BoundAtClause clause("TIMESTAMP", Value("it's a quote's world"));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (TIMESTAMP => 'it''s a quote''s world')");
}

TEST_CASE("RenderAtClauseSQL - value that is just a single quote", "[duckhog][at-clause]") {
	BoundAtClause clause("TIMESTAMP", Value("'"));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (TIMESTAMP => '''')");
}

// ============================================================
// Unit string preservation
// ============================================================

TEST_CASE("RenderAtClauseSQL - unit string is preserved verbatim", "[duckhog][at-clause]") {
	BoundAtClause clause("MY_CUSTOM_UNIT", Value::INTEGER(42));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (MY_CUSTOM_UNIT => 42)");
}

TEST_CASE("RenderAtClauseSQL - lowercase unit preserved", "[duckhog][at-clause]") {
	BoundAtClause clause("version", Value::INTEGER(5));
	REQUIRE(RenderAtClauseSQL(clause) == "AT (version => 5)");
}
