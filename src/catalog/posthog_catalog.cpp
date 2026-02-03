//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_catalog.cpp
//
//===----------------------------------------------------------------------===//

#include "catalog/posthog_catalog.hpp"
#include "catalog/posthog_schema_entry.hpp"
#include "utils/posthog_logger.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/database_size.hpp"

#include <cctype>

namespace duckdb {

namespace {

bool IsConnectionFailureMessage(const std::string &message) {
    std::string lower;
    lower.reserve(message.size());
    for (unsigned char ch : message) {
        lower.push_back(static_cast<char>(std::tolower(ch)));
    }
    return lower.find("failed to connect") != std::string::npos ||
           lower.find("connection refused") != std::string::npos ||
           lower.find("unavailable") != std::string::npos ||
           lower.find("timed out") != std::string::npos;
}

} // namespace

PostHogCatalog::PostHogCatalog(AttachedDatabase &db, const string &name, PostHogConnectionConfig config)
    : Catalog(db), database_name_(name), config_(std::move(config)) {
}

PostHogCatalog::~PostHogCatalog() = default;

void PostHogCatalog::Initialize(bool load_builtin) {
    // Log attachment attempt
    POSTHOG_LOG_INFO("Connecting to remote database: %s", config_.database.c_str());
    POSTHOG_LOG_INFO("Flight server: %s", config_.flight_server.c_str());
    POSTHOG_LOG_DEBUG("Token: %s", config_.token.empty() ? "(none)" : "(provided)");

    // Create the Flight SQL client (Milestone 3)
    try {
        flight_client_ = make_uniq<PostHogFlightClient>(config_.flight_server, config_.token);
        flight_client_->Authenticate();
        POSTHOG_LOG_INFO("Initialized Flight SQL client");
        auto ping_status = flight_client_->Ping();
        if (ping_status.ok()) {
            POSTHOG_LOG_INFO("Flight server is reachable");
        } else {
            POSTHOG_LOG_WARN("Flight server not reachable yet: %s", ping_status.ToString().c_str());
            POSTHOG_LOG_WARN("Catalog created in disconnected mode. Queries will fail until connection is restored.");
        }
    } catch (const std::exception &e) {
        // Log the error but don't throw - allow catalog to be created even if connection fails
        // This enables testing the extension without a running server
        POSTHOG_LOG_WARN("Failed to connect to Flight server: %s", e.what());
        POSTHOG_LOG_WARN("Catalog created in disconnected mode. Queries will fail until connection is restored.");
    }
}

optional_ptr<CatalogEntry> PostHogCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
    throw NotImplementedException("PostHog: CreateSchema not supported on remote database");
}

//===----------------------------------------------------------------------===//
// Schema Loading (Lazy)
//===----------------------------------------------------------------------===//

void PostHogCatalog::LoadSchemasIfNeeded() {
    std::lock_guard<std::mutex> lock(schemas_mutex_);

    // Check if cache is still valid
    if (schemas_loaded_) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - schemas_loaded_at_).count();
        if (elapsed < CACHE_TTL_SECONDS) {
            return; // Cache is still valid
        }
        // Cache expired, need to refresh
        POSTHOG_LOG_DEBUG("Schema cache expired, refreshing...");
    }

    if (!IsConnected()) {
        POSTHOG_LOG_DEBUG("Cannot load schemas: not connected");
        return;
    }

    try {
        POSTHOG_LOG_DEBUG("Loading schemas from remote server...");
        // Query all remote catalogs by default. The local attached database name (e.g. "remote")
        // is a DuckDB concept and does not necessarily match the remote Flight SQL catalog_name.
        auto schema_names = flight_client_->ListSchemas("");

        // Create schema entries (don't clear existing ones to preserve loaded tables)
        for (const auto &schema_name : schema_names) {
            if (schema_cache_.find(schema_name) == schema_cache_.end()) {
                CreateSchemaEntry(schema_name);
            }
        }

        schemas_loaded_ = true;
        schemas_loaded_at_ = std::chrono::steady_clock::now();
        POSTHOG_LOG_INFO("Loaded %zu schemas", schema_names.size());

    } catch (const std::exception &e) {
        POSTHOG_LOG_ERROR("Failed to load schemas: %s", e.what());
        if (IsConnectionFailureMessage(e.what())) {
            throw CatalogException("PostHog: Not connected to remote server.");
        }
    }
}

void PostHogCatalog::CreateSchemaEntry(const string &schema_name) {
    auto schema_info = make_uniq<CreateSchemaInfo>();
    schema_info->schema = schema_name;
    schema_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

    auto schema_entry = make_uniq<PostHogSchemaEntry>(*this, *schema_info, *this);
    schema_cache_[schema_name] = std::move(schema_entry);
}

optional_ptr<PostHogSchemaEntry> PostHogCatalog::GetOrCreateSchema(const string &schema_name) {
    std::lock_guard<std::mutex> lock(schemas_mutex_);

    auto it = schema_cache_.find(schema_name);
    if (it != schema_cache_.end()) {
        return it->second.get();
    }

    // Schema not in cache - try to create it if we're connected
    if (!IsConnected()) {
        return nullptr;
    }

    // Create the schema entry on-demand
    CreateSchemaEntry(schema_name);
    it = schema_cache_.find(schema_name);
    if (it != schema_cache_.end()) {
        return it->second.get();
    }
    return nullptr;
}

void PostHogCatalog::RefreshSchemas() {
    std::lock_guard<std::mutex> lock(schemas_mutex_);
    schemas_loaded_ = false;
    // Also refresh tables in each schema
    for (auto &entry : schema_cache_) {
        entry.second->RefreshTables();
    }
}

//===----------------------------------------------------------------------===//
// Schema Scanning and Lookup
//===----------------------------------------------------------------------===//

void PostHogCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    // If not connected, no schemas are available
    if (!IsConnected()) {
        return;
    }

    LoadSchemasIfNeeded();

    std::lock_guard<std::mutex> lock(schemas_mutex_);
    for (auto &entry : schema_cache_) {
        callback(*entry.second);
    }
}

optional_ptr<SchemaCatalogEntry> PostHogCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
    // Check connection status
    if (!IsConnected()) {
        if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
            throw CatalogException("PostHog: Not connected to remote server. Schema lookup failed for '%s'.",
                                   schema_lookup.GetEntryName());
        }
        return nullptr;
    }

    // Load schemas if needed
    LoadSchemasIfNeeded();

    // Look up the schema
    auto schema = GetOrCreateSchema(schema_lookup.GetEntryName());
    if (!schema) {
        if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
            throw CatalogException("PostHog: Schema '%s' not found in remote database.", schema_lookup.GetEntryName());
        }
        return nullptr;
    }

    // Return as SchemaCatalogEntry pointer (PostHogSchemaEntry inherits from SchemaCatalogEntry)
    return schema.get();
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
    // Return the flight server endpoint or control plane URL
    if (!config_.flight_server.empty()) {
        return config_.flight_server;
    }
    return config_.control_plane;
}

void PostHogCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    throw NotImplementedException("PostHog: DROP SCHEMA not yet implemented");
}

} // namespace duckdb
