//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_sql_utils.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_sql_utils.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"

namespace duckdb {

/// Serialize a Value to valid SQL.
///
/// DuckDB's Value::ToSQLString() falls through to ToString() for MAP, which
/// produces the display format {k=v, ...} — not valid SQL.  We emit
/// MAP {'key': val, ...} instead, recursing for nested types.  Also used by
/// FilterToSQL so pushed-down constants on MAP/STRUCT/LIST columns serialize
/// correctly.
static string ValueToSQL(const Value &val) {
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
			sql += ValueToSQL(kv[0]);
			sql += ": ";
			// value
			sql += ValueToSQL(kv[1]);
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
			sql += ValueToSQL(children[i]);
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
			sql += ValueToSQL(children[i]);
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
			sql += ValueToSQL(chunk.GetValue(col_idx, row_idx));
		}
		sql += ")";
	}

	sql += on_conflict_clause;
	sql += ";";
	return sql;
}

//===----------------------------------------------------------------------===//
// Filter pushdown translation
//===----------------------------------------------------------------------===//

namespace {

string ComparisonOperatorToSQL(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "<>";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "IS DISTINCT FROM";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "IS NOT DISTINCT FROM";
	default:
		throw NotImplementedException("PostHog filter pushdown: unsupported comparison operator");
	}
}

} // namespace

string FilterToSQL(const TableFilter &filter, const string &column_expr) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &cmp = filter.Cast<ConstantFilter>();
		return column_expr + " " + ComparisonOperatorToSQL(cmp.comparison_type) + " " + ValueToSQL(cmp.constant);
	}
	case TableFilterType::IS_NULL:
		return column_expr + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return column_expr + " IS NOT NULL";
	case TableFilterType::CONJUNCTION_AND:
	case TableFilterType::CONJUNCTION_OR: {
		// Empty-string convention: a child returning "" means "no SQL
		// constraint, treat as TRUE". AND drops TRUE children; OR with a TRUE
		// child collapses to TRUE (return ""). Children that throw propagate
		// — silently dropping a translatable-but-unhandled child of an AND
		// would emit a more-permissive WHERE, and the residual filter does
		// not always reapply (see PostHogRemoteScan::GetFunction).
		const bool is_and = filter.filter_type == TableFilterType::CONJUNCTION_AND;
		auto &conj = static_cast<const ConjunctionFilter &>(filter);
		const char *sep = is_and ? " AND " : " OR ";
		string out;
		for (auto &child : conj.child_filters) {
			string child_sql = FilterToSQL(*child, column_expr);
			if (child_sql.empty()) {
				if (!is_and) {
					return string();
				}
				continue;
			}
			if (!out.empty()) {
				out += sep;
			}
			out += child_sql;
		}
		if (out.empty()) {
			return out;
		}
		return "(" + out + ")";
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		if (in_filter.values.empty()) {
			return "FALSE";
		}
		string out = column_expr + " IN (";
		for (idx_t i = 0; i < in_filter.values.size(); i++) {
			if (i > 0) {
				out += ", ";
			}
			out += ValueToSQL(in_filter.values[i]);
		}
		out += ")";
		return out;
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		if (!struct_filter.child_filter) {
			return string();
		}
		// Match DuckDB's StructFilter::ToString: positional access when
		// child_name is empty, dotted-name access otherwise.
		string child_expr;
		if (struct_filter.child_name.empty()) {
			child_expr = "struct_extract_at(" + column_expr + ", " + std::to_string(struct_filter.child_idx + 1) + ")";
		} else {
			child_expr = column_expr + "." + QuoteIdent(struct_filter.child_name);
		}
		return FilterToSQL(*struct_filter.child_filter, child_expr);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &opt = filter.Cast<OptionalFilter>();
		if (!opt.child_filter) {
			return string();
		}
		try {
			return FilterToSQL(*opt.child_filter, column_expr);
		} catch (const NotImplementedException &) {
			return string();
		}
	}
	case TableFilterType::DYNAMIC_FILTER:
		// Dynamic filter values aren't knowable at SQL-generation time;
		// the residual filter above the scan will apply them.
		return string();
	default:
		throw NotImplementedException("PostHog filter pushdown: unsupported filter type");
	}
}

} // namespace duckdb
