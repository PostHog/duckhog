//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_catalog.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "utils/connection_string.hpp"
#include "flight/flight_client.hpp"

#include <memory>
#include <unordered_map>
#include <mutex>
#include <chrono>

namespace duckdb {

class PostHogSchemaEntry;

class PostHogCatalog : public Catalog {
public:
	// Constructor for multi-catalog attach: each PostHogCatalog maps to exactly one remote catalog
	PostHogCatalog(AttachedDatabase &db, const string &name, PostHogConnectionConfig config,
	               const string &remote_catalog);
	~PostHogCatalog() override;

public:
	void Initialize(bool load_builtin) override;

	string GetCatalogType() override {
		return "hog";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	bool InMemory() override;
	string GetDBPath() override;

	// Accessors for configuration
	const PostHogConnectionConfig &GetConfig() const {
		return config_;
	}
	const string &GetDatabaseName() const {
		return database_name_;
	}

	// Get the remote catalog name this instance maps to
	const string &GetRemoteCatalog() const {
		return remote_catalog_;
	}

	// Access to Flight client for query execution
	PostHogFlightClient &GetFlightClient() {
		return *flight_client_;
	}

	// Check if connected to remote server
	bool IsConnected() const {
		return flight_client_ && flight_client_->IsConnected() && flight_client_->IsAuthenticated();
	}

	// Force refresh of schema cache
	void RefreshSchemas();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	// Load schemas from remote server (lazy loading)
	void LoadSchemasIfNeeded();

	// Create a schema entry for a remote schema
	void CreateSchemaEntry(const string &schema_name);

	// Get or create a schema entry
	optional_ptr<PostHogSchemaEntry> GetOrCreateSchema(const string &schema_name);

private:
	string database_name_;
	string remote_catalog_; // The remote catalog this instance maps to
	PostHogConnectionConfig config_;
	unique_ptr<PostHogFlightClient> flight_client_;

	// Schema cache (keyed by schema name only, since this catalog maps to one remote catalog)
	mutable std::mutex schemas_mutex_;
	bool schemas_loaded_ = false;
	std::chrono::steady_clock::time_point schemas_loaded_at_;
	std::unordered_map<string, unique_ptr<PostHogSchemaEntry>> schema_cache_;

	// Cache TTL (5 minutes by default)
	static constexpr int64_t CACHE_TTL_SECONDS = 300;
};

} // namespace duckdb
