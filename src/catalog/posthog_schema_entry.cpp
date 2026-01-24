//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_schema_entry.cpp
//
// Virtual schema entry implementation for remote PostHog schemas
//===----------------------------------------------------------------------===//

#include "catalog/posthog_schema_entry.hpp"
#include "catalog/posthog_catalog.hpp"
#include "catalog/posthog_table_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include <arrow/c/bridge.h>

#include <iostream>

namespace duckdb {

static void PopulateTableSchemaFromArrow(DBConfig &config, const std::shared_ptr<arrow::Schema> &schema,
                                         vector<string> &names, vector<LogicalType> &types) {
    ArrowSchema arrow_schema;
    auto status = arrow::ExportSchema(*schema, &arrow_schema);
    if (!status.ok()) {
        throw IOException("PostHog: Failed to export Arrow schema: " + status.ToString());
    }

    ArrowTableSchema arrow_table;
    ArrowTableFunction::PopulateArrowTableSchema(config, arrow_table, arrow_schema);
    names = arrow_table.GetNames();
    types = arrow_table.GetTypes();

    if (arrow_schema.release) {
        arrow_schema.release(&arrow_schema);
    }
}

PostHogSchemaEntry::PostHogSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, PostHogCatalog &posthog_catalog)
    : SchemaCatalogEntry(catalog, info), posthog_catalog_(posthog_catalog) {
}

PostHogSchemaEntry::~PostHogSchemaEntry() = default;

//===----------------------------------------------------------------------===//
// Create Operations (Not Supported for Remote)
//===----------------------------------------------------------------------===//

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    throw NotImplementedException("PostHog: CREATE TABLE not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
    throw NotImplementedException("PostHog: CREATE FUNCTION not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
    throw NotImplementedException("PostHog: CREATE INDEX not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
    throw NotImplementedException("PostHog: CREATE VIEW not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
    throw NotImplementedException("PostHog: CREATE SEQUENCE not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                   CreateTableFunctionInfo &info) {
    throw NotImplementedException("PostHog: CREATE TABLE FUNCTION not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                  CreateCopyFunctionInfo &info) {
    throw NotImplementedException("PostHog: CREATE COPY FUNCTION not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                    CreatePragmaFunctionInfo &info) {
    throw NotImplementedException("PostHog: CREATE PRAGMA FUNCTION not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
    throw NotImplementedException("PostHog: CREATE COLLATION not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
    throw NotImplementedException("PostHog: CREATE TYPE not supported on remote database");
}

//===----------------------------------------------------------------------===//
// Alter/Drop Operations
//===----------------------------------------------------------------------===//

void PostHogSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
    throw NotImplementedException("PostHog: ALTER not supported on remote database");
}

void PostHogSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    throw NotImplementedException("PostHog: DROP not supported on remote database");
}

//===----------------------------------------------------------------------===//
// Table Loading (Lazy)
//===----------------------------------------------------------------------===//

void PostHogSchemaEntry::LoadTablesIfNeeded() {
    // Note: This method is called while holding tables_mutex_ from the caller
    // or should be called with the lock already held

    // Check if cache is still valid
    if (tables_loaded_) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - tables_loaded_at_).count();
        if (elapsed < CACHE_TTL_SECONDS) {
            return; // Cache is still valid
        }
        // Cache expired, need to refresh
        std::cerr << "[PostHog] Table cache expired for schema " << name << ", refreshing..." << std::endl;
    }

    if (!posthog_catalog_.IsConnected()) {
        std::cerr << "[PostHog] Cannot load tables: not connected" << std::endl;
        return;
    }

    try {
        std::cerr << "[PostHog] Loading tables for schema: " << name << std::endl;
        auto &client = posthog_catalog_.GetFlightClient();
        auto table_names = client.ListTables(name);

        // Create entries for tables not already in cache
        for (const auto &table_name : table_names) {
            if (table_cache_.find(table_name) == table_cache_.end()) {
                CreateTableEntry(table_name);
            }
        }

        tables_loaded_ = true;
        tables_loaded_at_ = std::chrono::steady_clock::now();
        std::cerr << "[PostHog] Loaded " << table_names.size() << " tables for schema " << name << std::endl;

    } catch (const std::exception &e) {
        std::cerr << "[PostHog] Failed to load tables for schema " << name << ": " << e.what() << std::endl;
    }
}

void PostHogSchemaEntry::CreateTableEntry(const string &table_name) {
    // Note: Called with tables_mutex_ already held

    if (!posthog_catalog_.IsConnected()) {
        return;
    }

    // Skip if already exists
    if (table_cache_.find(table_name) != table_cache_.end()) {
        return;
    }

    try {
        auto &client = posthog_catalog_.GetFlightClient();
        auto arrow_schema = client.GetTableSchema(name, table_name);

        vector<string> column_names;
        vector<LogicalType> column_types;
        PopulateTableSchemaFromArrow(DBConfig::GetConfig(posthog_catalog_.GetDatabase()), arrow_schema, column_names,
                                     column_types);

        auto create_info = make_uniq<CreateTableInfo>(*this, table_name);
        for (idx_t i = 0; i < column_names.size(); i++) {
            create_info->columns.AddColumn(ColumnDefinition(column_names[i], column_types[i]));
        }
        create_info->columns.Finalize();

        auto table_entry = make_uniq<PostHogTableEntry>(catalog, *this, *create_info, posthog_catalog_);
        table_cache_.emplace(table_name, std::move(table_entry));
    } catch (const std::exception &e) {
        std::cerr << "[PostHog] Failed to create table entry for " << name << "." << table_name << ": " << e.what()
                  << std::endl;
    }
}

optional_ptr<PostHogTableEntry> PostHogSchemaEntry::GetOrCreateTable(const string &table_name) {
    // Note: Called with tables_mutex_ already held

    auto it = table_cache_.find(table_name);
    if (it != table_cache_.end()) {
        return it->second.get();
    }

    // Table not in cache - try to create it if we're connected
    if (!posthog_catalog_.IsConnected()) {
        return nullptr;
    }

    // Create the table entry on-demand
    CreateTableEntry(table_name);
    it = table_cache_.find(table_name);
    if (it != table_cache_.end()) {
        return it->second.get();
    }
    return nullptr;
}

void PostHogSchemaEntry::RefreshTables() {
    std::lock_guard<std::mutex> lock(tables_mutex_);
    tables_loaded_ = false;
}

//===----------------------------------------------------------------------===//
// Scan and Lookup
//===----------------------------------------------------------------------===//

void PostHogSchemaEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry &)> &callback) {
    if (type != CatalogType::TABLE_ENTRY) {
        // We only have tables in remote schemas
        return;
    }

    std::lock_guard<std::mutex> lock(tables_mutex_);
    LoadTablesIfNeeded();

    for (auto &entry : table_cache_) {
        callback(*entry.second);
    }
}

void PostHogSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }

    std::lock_guard<std::mutex> lock(tables_mutex_);
    LoadTablesIfNeeded();

    for (auto &entry : table_cache_) {
        callback(*entry.second);
    }
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                          const EntryLookupInfo &lookup_info) {
    if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(tables_mutex_);
    LoadTablesIfNeeded();

    // Try to get from cache, or create on-demand
    auto table = GetOrCreateTable(lookup_info.GetEntryName());
    if (table) {
        return table.get();
    }
    return nullptr;
}

} // namespace duckdb
