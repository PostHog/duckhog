//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_table_entry.cpp
//
// Virtual table entry implementation that proxies to a remote PostHog table
//===----------------------------------------------------------------------===//

#include "catalog/posthog_table_entry.hpp"
#include "catalog/posthog_catalog.hpp"
#include "catalog/posthog_schema_entry.hpp"
#include "catalog/remote_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include <arrow/type.h>

#include <iostream>

namespace duckdb {

PostHogTableEntry::PostHogTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     PostHogCatalog &posthog_catalog, std::shared_ptr<arrow::Schema> arrow_schema)
    : TableCatalogEntry(catalog, schema, info), posthog_catalog_(posthog_catalog), schema_name_(schema.name),
      arrow_schema_(std::move(arrow_schema)) {
}

PostHogTableEntry::~PostHogTableEntry() = default;

unique_ptr<BaseStatistics> PostHogTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	// No statistics available for remote tables
	return nullptr;
}

TableFunction PostHogTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Create bind data for the remote scan
	vector<string> column_names;
	vector<LogicalType> column_types;

	for (auto &col : columns.Logical()) {
		column_names.push_back(col.Name());
		column_types.push_back(col.Type());
	}

	bind_data = PostHogRemoteScan::CreateBindData(posthog_catalog_, schema_name_, name, column_names, column_types,
	                                              arrow_schema_);

	return PostHogRemoteScan::GetFunction();
}

// Render a BoundAtClause as a SQL fragment for the remote query.
// DuckLake supports AT (VERSION => <int>) and AT (TIMESTAMP => '<ts>').
// Safety: Unit() is always "TIMESTAMP" or "VERSION" — the grammar (select.y at_unit rule)
// hardcodes these as string literals, so no user-controlled text reaches the unit string.
string RenderAtClauseSQL(const BoundAtClause &at_clause) {
	const auto &unit = at_clause.Unit();
	const auto &val = at_clause.GetValue();
	// Integer-typed values (VERSION) render unquoted; everything else is single-quoted
	// with embedded single quotes escaped as ''.
	if (val.type().IsIntegral()) {
		return "AT (" + unit + " => " + val.ToString() + ")";
	}
	auto str = StringUtil::Replace(val.ToString(), "'", "''");
	return "AT (" + unit + " => '" + str + "')";
}

TableFunction PostHogTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                 const EntryLookupInfo &lookup_info) {
	auto func = GetScanFunction(context, bind_data);

	auto at_clause = lookup_info.GetAtClause();
	if (at_clause) {
		auto &scan_bind = bind_data->Cast<PostHogRemoteScanBindData>();
		scan_bind.at_clause_sql = RenderAtClauseSQL(*at_clause);
	}

	return func;
}

TableStorageInfo PostHogTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	// Remote tables - we don't have local storage info
	info.cardinality = 0; // Unknown
	return info;
}

void PostHogTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                              LogicalUpdate &update, ClientContext &context) {
	TableCatalogEntry::BindUpdateConstraints(binder, get, proj, update, context);
}

} // namespace duckdb
