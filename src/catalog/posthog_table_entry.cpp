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
#include "flight/arrow_conversion.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"

#include <iostream>

namespace duckdb {

PostHogTableEntry::PostHogTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     PostHogCatalog &posthog_catalog)
    : TableCatalogEntry(catalog, schema, info), posthog_catalog_(posthog_catalog), schema_name_(schema.name) {
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

    bind_data = PostHogRemoteScan::CreateBindData(posthog_catalog_, schema_name_, name, column_names, column_types);

    return PostHogRemoteScan::GetFunction();
}

TableStorageInfo PostHogTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo info;
    // Remote tables - we don't have local storage info
    info.cardinality = 0; // Unknown
    return info;
}

void PostHogTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                               LogicalUpdate &update, ClientContext &context) {
    throw NotImplementedException("PostHog: UPDATE operations are not supported on remote tables");
}

} // namespace duckdb
