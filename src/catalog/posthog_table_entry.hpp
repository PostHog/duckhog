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

#include <memory>

namespace arrow {
class Schema;
} // namespace arrow

namespace duckdb {

class PostHogCatalog;
class PostHogSchemaEntry;

class BoundAtClause;

// Render a BoundAtClause as a SQL fragment for the remote query, e.g.
// "AT (VERSION => 1)" or "AT (TIMESTAMP => '2024-01-15 10:30:00')".
string RenderAtClauseSQL(const BoundAtClause &at_clause);

class PostHogTableEntry : public TableCatalogEntry {
public:
	PostHogTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                  PostHogCatalog &posthog_catalog, std::shared_ptr<arrow::Schema> arrow_schema);
	~PostHogTableEntry() override;

public:
	//===--------------------------------------------------------------------===//
	// TableCatalogEntry Interface
	//===--------------------------------------------------------------------===//

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup_info) override;

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

	// Get the cached Arrow schema from catalog creation
	const std::shared_ptr<arrow::Schema> &GetArrowSchema() const {
		return arrow_schema_;
	}

private:
	PostHogCatalog &posthog_catalog_;
	string schema_name_;
	std::shared_ptr<arrow::Schema> arrow_schema_;
};

} // namespace duckdb
