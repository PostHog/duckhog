//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_catalog.cpp
//
//===----------------------------------------------------------------------===//

#include "catalog/posthog_catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/database_size.hpp"

#include <iostream>

namespace duckdb {

PostHogCatalog::PostHogCatalog(AttachedDatabase &db, const string &name, PostHogConnectionConfig config)
    : Catalog(db), database_name_(name), config_(std::move(config)) {
}

PostHogCatalog::~PostHogCatalog() = default;

void PostHogCatalog::Initialize(bool load_builtin) {
    // Log successful attachment (Milestone 2 requirement)
    // In production, this would be where we connect to the Flight server
    std::cerr << "[PostHog] Attached to remote database: " << config_.database << std::endl;
    std::cerr << "[PostHog] Endpoint: " << config_.endpoint << std::endl;
    std::cerr << "[PostHog] Token: " << (config_.token.empty() ? "(none)" : "(provided)") << std::endl;
}

optional_ptr<CatalogEntry> PostHogCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
    throw NotImplementedException("PostHog: CreateSchema not yet implemented (requires Flight connection)");
}

void PostHogCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    // For Milestone 2, we don't have any schemas yet
    // This will be populated when we connect to the Flight server in Milestone 3
}

optional_ptr<SchemaCatalogEntry> PostHogCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
    // For Milestone 2, no schemas are available yet
    if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
        throw CatalogException("PostHog: Schema lookup not yet implemented (requires Flight connection)");
    }
    return nullptr;
}

PhysicalOperator &PostHogCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             optional_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("PostHog: INSERT not yet implemented");
}

PhysicalOperator &PostHogCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
    throw NotImplementedException("PostHog: CREATE TABLE AS not yet implemented");
}

PhysicalOperator &PostHogCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
    throw NotImplementedException("PostHog: DELETE not yet implemented");
}

PhysicalOperator &PostHogCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                             PhysicalOperator &plan) {
    throw NotImplementedException("PostHog: UPDATE not yet implemented");
}

DatabaseSize PostHogCatalog::GetDatabaseSize(ClientContext &context) {
    DatabaseSize size;
    size.free_blocks = 0;
    size.total_blocks = 0;
    size.used_blocks = 0;
    size.wal_size = 0;
    size.block_size = 0;
    size.bytes = 0;
    return size;
}

bool PostHogCatalog::InMemory() {
    return false;  // This is a remote database
}

string PostHogCatalog::GetDBPath() {
    return config_.endpoint;
}

void PostHogCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    throw NotImplementedException("PostHog: DROP SCHEMA not yet implemented");
}

} // namespace duckdb
