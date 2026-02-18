//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_stub_catalog.cpp
//
//===----------------------------------------------------------------------===//

#include "catalog/posthog_stub_catalog.hpp"
#include "utils/posthog_logger.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

PostHogStubCatalog::PostHogStubCatalog(AttachedDatabase &db, const string &name) : Catalog(db), database_name_(name) {
}

PostHogStubCatalog::~PostHogStubCatalog() = default;

void PostHogStubCatalog::Initialize(bool load_builtin) {
	POSTHOG_LOG_INFO("Stub catalog '%s' initialized (use '%s_<catalog>' for queries)", database_name_.c_str(),
	                 database_name_.c_str());
}

optional_ptr<CatalogEntry> PostHogStubCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("PostHog: Cannot create schema on '%s'. Use '%s_<catalog>' instead.", database_name_,
	                              database_name_);
}

void PostHogStubCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	// Return no schemas - this is intentionally empty
}

optional_ptr<SchemaCatalogEntry> PostHogStubCatalog::LookupSchema(CatalogTransaction transaction,
                                                                  const EntryLookupInfo &schema_lookup,
                                                                  OnEntryNotFound if_not_found) {
	// Always throw a helpful error - this stub catalog has no schemas
	// We throw regardless of if_not_found to provide a clear message before DuckDB's generic error
	throw CatalogException("PostHog: '%s' is a stub catalog with no tables. "
	                       "Use '%s_<catalog>' instead (e.g., '%s_default').",
	                       database_name_, database_name_, database_name_);
}

// Defense-in-depth: LookupSchema() always throws before any of these DML
// plan overrides are reached, so the user sees the schema-level error first.
// These overrides exist as a safety net in case the schema lookup path is
// ever changed to not throw (e.g., supporting ON_ENTRY_NOT_FOUND).

PhysicalOperator &PostHogStubCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("PostHog: Cannot insert into stub catalog '%s'. Use '%s_<catalog>' instead.",
	                              database_name_, database_name_);
}

PhysicalOperator &PostHogStubCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                        LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("PostHog: Cannot create table in stub catalog '%s'. Use '%s_<catalog>' instead.",
	                              database_name_, database_name_);
}

PhysicalOperator &PostHogStubCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException("PostHog: Cannot delete from stub catalog '%s'. Use '%s_<catalog>' instead.",
	                              database_name_, database_name_);
}

PhysicalOperator &PostHogStubCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalUpdate &op, PhysicalOperator &plan) {
	(void)plan;
	return PlanUpdate(context, planner, op);
}

PhysicalOperator &PostHogStubCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalUpdate &op) {
	throw NotImplementedException("PostHog: Cannot update stub catalog '%s'. Use '%s_<catalog>' instead.",
	                              database_name_, database_name_);
}

PhysicalOperator &PostHogStubCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalMergeInto &op, PhysicalOperator &plan) {
	throw NotImplementedException("PostHog: Cannot merge into stub catalog '%s'. Use '%s_<catalog>' instead.",
	                              database_name_, database_name_);
}

DatabaseSize PostHogStubCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

bool PostHogStubCatalog::InMemory() {
	return true; // Stub has no actual storage
}

string PostHogStubCatalog::GetDBPath() {
	return ""; // No path for stub
}

void PostHogStubCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("PostHog: Cannot drop schema from stub catalog '%s'.", database_name_);
}

} // namespace duckdb
