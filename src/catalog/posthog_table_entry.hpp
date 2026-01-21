//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_table_entry.hpp
//
// Virtual table entry that proxies to a remote PostHog table
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

class PostHogCatalog;
class PostHogSchemaEntry;

class PostHogTableEntry : public TableCatalogEntry {
public:
    PostHogTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                      PostHogCatalog &posthog_catalog);
    ~PostHogTableEntry() override;

public:
    //===--------------------------------------------------------------------===//
    // TableCatalogEntry Interface
    //===--------------------------------------------------------------------===//

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
                               ClientContext &context) override;

    //===--------------------------------------------------------------------===//
    // PostHog-specific methods
    //===--------------------------------------------------------------------===//

    // Get the schema name for this table
    const string &GetSchemaName() const {
        return schema_name_;
    }

    // Get the parent PostHog catalog
    PostHogCatalog &GetPostHogCatalog() {
        return posthog_catalog_;
    }

private:
    PostHogCatalog &posthog_catalog_;
    string schema_name_;
};

} // namespace duckdb
