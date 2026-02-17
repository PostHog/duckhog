//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_sql_utils.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_sql_utils.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

string BuildInsertSQL(const string &qualified_table, const vector<string> &column_names, const DataChunk &chunk,
                      const string &on_conflict_clause) {
	string sql = "INSERT INTO " + qualified_table;
	if (column_names.empty()) {
		if (chunk.size() != 1) {
			throw NotImplementedException("PostHog: multi-row INSERT DEFAULT VALUES is not yet implemented");
		}
		sql += " DEFAULT VALUES";
		sql += on_conflict_clause;
		sql += ";";
		return sql;
	}
	if (chunk.ColumnCount() != column_names.size()) {
		throw InternalException("PostHog: insert chunk has %llu columns but table has %llu insert columns",
		                        chunk.ColumnCount(), column_names.size());
	}

	sql += " (";
	for (idx_t col_idx = 0; col_idx < column_names.size(); col_idx++) {
		if (col_idx > 0) {
			sql += ", ";
		}
		sql += QuoteIdent(column_names[col_idx]);
	}
	sql += ") VALUES ";

	for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		if (row_idx > 0) {
			sql += ", ";
		}
		sql += "(";
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			if (col_idx > 0) {
				sql += ", ";
			}
			sql += chunk.GetValue(col_idx, row_idx).ToSQLString();
		}
		sql += ")";
	}

	sql += on_conflict_clause;
	sql += ";";
	return sql;
}

} // namespace duckdb
