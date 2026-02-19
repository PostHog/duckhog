//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_schema_entry.hpp
//
// Virtual schema entry for remote PostHog schemas
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

#include <unordered_map>
#include <mutex>
#include <chrono>

namespace duckdb {

class PostHogCatalog;
class PostHogTableEntry;

class PostHogSchemaEntry : public SchemaCatalogEntry {
public:
	PostHogSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, PostHogCatalog &posthog_catalog);
	~PostHogSchemaEntry() override;

public:
	//===--------------------------------------------------------------------===//
	// Catalog Entry Interface
	//===--------------------------------------------------------------------===//

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	void DropEntry(ClientContext &context, DropInfo &info) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	//===--------------------------------------------------------------------===//
	// PostHog-specific methods
	//===--------------------------------------------------------------------===//

	// Get the parent PostHog catalog
	PostHogCatalog &GetPostHogCatalog() {
		return posthog_catalog_;
	}

	// Force refresh of table cache
	void RefreshTables();

	// Check if tables have been loaded
	bool TablesLoaded() const {
		return tables_loaded_;
	}

private:
	// Load tables from remote server (lazy loading)
	void LoadTablesIfNeeded();

	// Create a table entry for a remote table
	void CreateTableEntry(const string &table_name);

	// Get or create a table entry
	optional_ptr<PostHogTableEntry> GetOrCreateTable(const string &table_name);

	PostHogCatalog &posthog_catalog_;

	// Table cache (using simple map instead of CatalogSet for simplicity)
	mutable std::mutex tables_mutex_;
	bool tables_loaded_ = false;
	std::chrono::steady_clock::time_point tables_loaded_at_;
	std::unordered_map<string, unique_ptr<PostHogTableEntry>> table_cache_;

	// Table function proxy cache (e.g. snapshots(), table_changes())
	std::unordered_map<string, unique_ptr<TableFunctionCatalogEntry>> table_function_cache_;

	// Cache TTL (5 minutes by default)
	static constexpr int64_t CACHE_TTL_SECONDS = 300;
};

} // namespace duckdb
