//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_sql_utils.hpp
//
// Shared SQL generation utilities for remote DML operators.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/table_filter.hpp"

#include <string>
#include <vector>

namespace duckdb {

inline string QuoteIdent(const string &ident) {
	return KeywordHelper::WriteOptionallyQuoted(ident);
}

inline string QualifyRemoteTableName(const string &remote_catalog, const string &schema, const string &table) {
	return QuoteIdent(remote_catalog) + "." + QuoteIdent(schema) + "." + QuoteIdent(table);
}

/// Build an INSERT INTO ... VALUES statement for a single DataChunk.
string BuildInsertSQL(const string &qualified_table, const vector<string> &column_names, const DataChunk &chunk,
                      const string &on_conflict_clause = "");

/// Translate a TableFilter into a SQL boolean expression on `column_expr` (a
/// quoted SQL identifier or expression). Returns an empty string for
/// optional/dynamic/unknown filters that can be safely skipped — the residual
/// filter operator above the scan will still apply them. Throws
/// NotImplementedException for filter shapes whose semantics cannot be
/// preserved by skipping.
string FilterToSQL(const TableFilter &filter, const string &column_expr);

} // namespace duckdb
