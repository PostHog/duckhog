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

} // namespace duckdb
