//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_catalog.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "utils/connection_string.hpp"
#include "flight/flight_client.hpp"

#include <memory>

namespace duckdb {

class PostHogCatalog : public Catalog {
public:
    PostHogCatalog(AttachedDatabase &db, const string &name, PostHogConnectionConfig config);
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

    // Access to Flight client for query execution
    PostHogFlightClient &GetFlightClient() {
        return *flight_client_;
    }

    // Check if connected to remote server
    bool IsConnected() const {
        return flight_client_ && flight_client_->IsConnected() && flight_client_->IsAuthenticated();
    }

private:
    void DropSchema(ClientContext &context, DropInfo &info) override;

private:
    string database_name_;
    PostHogConnectionConfig config_;
    unique_ptr<PostHogFlightClient> flight_client_;
};

} // namespace duckdb
