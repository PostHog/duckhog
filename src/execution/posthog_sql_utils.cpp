//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_sql_utils.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_sql_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

/// Serialize a Value to valid SQL for INSERT statements.
///
/// DuckDB's Value::ToSQLString() falls through to ToString() for MAP, which
/// produces the display format {k=v, ...} — not valid SQL.  We emit
/// MAP {'key': val, ...} instead, recursing for nested types.
static string ValueToInsertSQL(const Value &val) {
	if (val.IsNull()) {
		return val.ToSQLString();
	}
	auto type_id = val.type().id();
	if (type_id == LogicalTypeId::MAP) {
		auto &children = MapValue::GetChildren(val);
		// MAP is LIST(STRUCT(key K, value V)); each child is a {key, value} struct.
		string sql = "MAP {";
		for (idx_t i = 0; i < children.size(); i++) {
			auto &entry = children[i];
			auto &kv = StructValue::GetChildren(entry);
			// key
			sql += ValueToInsertSQL(kv[0]);
			sql += ": ";
			// value
			sql += ValueToInsertSQL(kv[1]);
			if (i < children.size() - 1) {
				sql += ", ";
			}
		}
		sql += "}";
		return sql;
	}
	if (type_id == LogicalTypeId::STRUCT) {
		auto &children = StructValue::GetChildren(val);
		auto &child_types = StructType::GetChildTypes(val.type());
		string sql = "{";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			sql += "'" + StringUtil::Replace(child_types[i].first, "'", "''") + "': ";
			sql += ValueToInsertSQL(children[i]);
		}
		sql += "}";
		return sql;
	}
	if (type_id == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(val);
		string sql = "[";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			sql += ValueToInsertSQL(children[i]);
		}
		sql += "]";
		return sql;
	}
	return val.ToSQLString();
}

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
			sql += ValueToInsertSQL(chunk.GetValue(col_idx, row_idx));
		}
		sql += ")";
	}

	sql += on_conflict_clause;
	sql += ";";
	return sql;
}

} // namespace duckdb
