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
    // Log attachment attempt
    std::cerr << "[PostHog] Connecting to remote database: " << config_.database << std::endl;
    std::cerr << "[PostHog] Endpoint: " << config_.endpoint << std::endl;
    std::cerr << "[PostHog] Token: " << (config_.token.empty() ? "(none)" : "(provided)") << std::endl;

    // Create the Flight SQL client (Milestone 3)
    try {
        flight_client_ = make_uniq<PostHogFlightClient>(config_.endpoint, config_.token);
        flight_client_->Authenticate();
        std::cerr << "[PostHog] Successfully connected to Flight server" << std::endl;
    } catch (const std::exception &e) {
        // Log the error but don't throw - allow catalog to be created even if connection fails
        // This enables testing the extension without a running server
        std::cerr << "[PostHog] Warning: Failed to connect to Flight server: " << e.what() << std::endl;
        std::cerr << "[PostHog] Catalog created in disconnected mode. Queries will fail until connection is restored."
                  << std::endl;
    }
}

optional_ptr<CatalogEntry> PostHogCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
    throw NotImplementedException("PostHog: CreateSchema not yet implemented (requires Flight connection)");
}

void PostHogCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    // If not connected, no schemas are available
    if (!IsConnected()) {
        return;
    }

    // TODO (Milestone 4): Implement schema scanning using flight_client_->ListSchemas()
    // This requires creating SchemaCatalogEntry objects for each remote schema
    // For now, schemas will be populated when Milestone 4 (Virtual Catalog) is implemented
}

optional_ptr<SchemaCatalogEntry> PostHogCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
    // Check connection status
    if (!IsConnected()) {
        if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
            throw CatalogException("PostHog: Not connected to remote server. Schema lookup failed.");
        }
        return nullptr;
    }

    // TODO (Milestone 4): Implement schema lookup using cached schema entries
    // This requires creating SchemaCatalogEntry objects for each remote schema
    // For now, return nullptr as schemas will be populated in Milestone 4
    if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
        throw CatalogException("PostHog: Schema lookup not yet fully implemented (Milestone 4)");
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
