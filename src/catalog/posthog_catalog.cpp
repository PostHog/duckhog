//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/posthog_catalog.cpp
//
//===----------------------------------------------------------------------===//

#include "catalog/posthog_catalog.hpp"
#include "catalog/posthog_schema_entry.hpp"
#include "catalog/posthog_table_entry.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution/posthog_create_table_as.hpp"
#include "execution/posthog_delete.hpp"
#include "execution/posthog_dml_rewriter.hpp"
#include "execution/posthog_insert.hpp"
#include "execution/posthog_merge.hpp"
#include "execution/posthog_update.hpp"
#include "storage/posthog_transaction.hpp"
#include "utils/posthog_logger.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <cctype>
#include <algorithm>

namespace duckdb {

namespace {

bool IsConnectionFailureMessage(const std::string &message) {
	std::string lower;
	lower.reserve(message.size());
	for (unsigned char ch : message) {
		lower.push_back(static_cast<char>(std::tolower(ch)));
	}
	return lower.find("failed to connect") != std::string::npos ||
	       lower.find("connection refused") != std::string::npos || lower.find("unavailable") != std::string::npos ||
	       lower.find("timed out") != std::string::npos;
}

} // namespace

PostHogCatalog::PostHogCatalog(AttachedDatabase &db, const string &name, PostHogConnectionConfig config,
                               const string &remote_catalog)
    : Catalog(db), database_name_(name), remote_catalog_(remote_catalog), config_(std::move(config)) {
}

PostHogCatalog::~PostHogCatalog() = default;

void PostHogCatalog::Initialize(bool load_builtin) {
	// Log attachment attempt
	POSTHOG_LOG_INFO("Attaching catalog '%s' -> remote catalog '%s'", database_name_.c_str(), remote_catalog_.c_str());
	POSTHOG_LOG_INFO("Flight server: %s", config_.flight_server.c_str());
	POSTHOG_LOG_DEBUG("User: %s", config_.user.empty() ? "(none)" : config_.user.c_str());
	if (config_.tls_skip_verify) {
		POSTHOG_LOG_WARN("TLS certificate verification disabled via tls_skip_verify=true");
	}

	// Create the Flight SQL client (Milestone 3)
	try {
		flight_client_ = make_uniq<PostHogFlightClient>(config_.flight_server, config_.user, config_.password,
		                                                config_.tls_skip_verify);
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
	if (!IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server. CREATE SCHEMA failed for '%s'.",
		                       info.schema.c_str());
	}

	std::optional<TransactionId> remote_txn_id;
	if (transaction.HasContext()) {
		remote_txn_id = PostHogTransaction::Get(transaction.GetContext(), *this).remote_txn_id;
	}

	auto copied = info.Copy();
	auto &remote_info = copied->Cast<CreateSchemaInfo>();
	remote_info.catalog = remote_catalog_;
	auto sql = remote_info.ToString();

	flight_client_->ExecuteUpdate(sql, remote_txn_id);

	std::lock_guard<std::mutex> lock(schemas_mutex_);
	auto it = schema_cache_.find(info.schema);
	if (it == schema_cache_.end()) {
		CreateSchemaEntry(info.schema);
		it = schema_cache_.find(info.schema);
	}
	schemas_loaded_ = true;
	schemas_loaded_at_ = std::chrono::steady_clock::now();
	return it != schema_cache_.end() ? it->second.get() : nullptr;
}

//===----------------------------------------------------------------------===//
// Schema Loading (Lazy)
//===----------------------------------------------------------------------===//

void PostHogCatalog::LoadSchemasIfNeeded() {
	bool should_load = false;
	{
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
		should_load = true;
	}

	if (!should_load) {
		return;
	}

	std::vector<PostHogDbSchemaInfo> schema_infos;
	try {
		POSTHOG_LOG_DEBUG("Loading schemas for remote catalog '%s'...", remote_catalog_.c_str());
		// Query schemas only for this catalog's remote_catalog_
		schema_infos = flight_client_->ListDbSchemas(remote_catalog_);

	} catch (const std::exception &e) {
		POSTHOG_LOG_ERROR("Failed to load schemas: %s", e.what());
		if (IsConnectionFailureMessage(e.what())) {
			throw CatalogException("PostHog: Not connected to remote server.");
		}
		return;
	}

	unordered_set<string> remote_schemas;
	remote_schemas.reserve(schema_infos.size());
	for (const auto &schema_info : schema_infos) {
		remote_schemas.insert(schema_info.schema_name);
	}

	std::lock_guard<std::mutex> lock(schemas_mutex_);

	// Prune schemas that no longer exist remotely.
	size_t pruned_count = 0;
	for (auto it = schema_cache_.begin(); it != schema_cache_.end();) {
		if (remote_schemas.find(it->first) == remote_schemas.end()) {
			it = schema_cache_.erase(it);
			pruned_count++;
			continue;
		}
		++it;
	}

	// Create schema entries for newly discovered schemas.
	size_t loaded_count = 0;
	for (const auto &schema_name : remote_schemas) {
		if (schema_cache_.find(schema_name) == schema_cache_.end()) {
			CreateSchemaEntry(schema_name);
			loaded_count++;
		}
	}

	schemas_loaded_ = true;
	schemas_loaded_at_ = std::chrono::steady_clock::now();
	POSTHOG_LOG_INFO("Loaded %zu schemas (pruned %zu) for remote catalog '%s'", loaded_count, pruned_count,
	                 remote_catalog_.c_str());
}

void PostHogCatalog::CreateSchemaEntry(const string &schema_name) {
	auto schema_info = make_uniq<CreateSchemaInfo>();
	schema_info->schema = schema_name;
	schema_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

	// Mark internal/metadata catalogs as internal for UI cleanliness
	// - DuckLake metadata catalogs: "__ducklake_metadata_*"
	// - DuckDB internal catalogs: "system", "temp"
	if (remote_catalog_ == "system" || remote_catalog_ == "temp" ||
	    remote_catalog_.find("__ducklake_metadata_") != string::npos) {
		schema_info->internal = true;
	}

	auto schema_entry = make_uniq<PostHogSchemaEntry>(*this, *schema_info, *this);
	schema_cache_[schema_name] = std::move(schema_entry);
}

optional_ptr<PostHogSchemaEntry> PostHogCatalog::GetOrCreateSchema(const string &schema_name) {
	std::lock_guard<std::mutex> lock(schemas_mutex_);

	// Look up in the schema cache for this catalog
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
	for (auto &schema_entry : schema_cache_) {
		schema_entry.second->RefreshTables();
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
	if (!IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server.");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW &&
	    op.on_conflict_info.action_type != OnConflictAction::NOTHING) {
		throw NotImplementedException("PostHog: INSERT ... ON CONFLICT currently supports only DO NOTHING");
	}
	if (op.return_chunk && op.on_conflict_info.action_type == OnConflictAction::NOTHING) {
		throw NotImplementedException("PostHog: INSERT ... ON CONFLICT DO NOTHING RETURNING is not yet implemented");
	}
	if (op.return_chunk && !op.column_index_map.empty()) {
		for (auto &column : op.table.GetColumns().Physical()) {
			auto mapped_index = op.column_index_map[column.Physical()];
			if (mapped_index == DConstants::INVALID_INDEX) {
				throw NotImplementedException(
				    "PostHog: INSERT ... RETURNING with omitted/default columns is not yet implemented");
			}
		}
	}
	if (op.on_conflict_info.on_conflict_condition || op.on_conflict_info.do_update_condition ||
	    !op.on_conflict_info.set_columns.empty()) {
		throw NotImplementedException("PostHog: INSERT ... ON CONFLICT currently supports only DO NOTHING");
	}
	if (!plan) {
		throw NotImplementedException("PostHog: INSERT without an input source is not yet implemented");
	}

	auto &table = op.table.Cast<PostHogTableEntry>();
	vector<string> column_names;
	vector<idx_t> return_input_index_map;
	if (op.return_chunk) {
		return_input_index_map.reserve(op.table.GetColumns().PhysicalColumnCount());
	}
	if (op.column_index_map.empty()) {
		idx_t input_idx = 0;
		for (auto &column : op.table.GetColumns().Physical()) {
			column_names.push_back(column.Name());
			if (op.return_chunk) {
				return_input_index_map.push_back(input_idx);
			}
			input_idx++;
		}
	} else {
		idx_t mapped_column_count = 0;
		for (auto &column : op.table.GetColumns().Physical()) {
			auto mapped_index = op.column_index_map[column.Physical()];
			if (mapped_index == DConstants::INVALID_INDEX) {
				continue;
			}
			mapped_column_count = std::max(mapped_column_count, mapped_index + 1);
		}
		column_names.resize(mapped_column_count);
		for (auto &column : op.table.GetColumns().Physical()) {
			auto mapped_index = op.column_index_map[column.Physical()];
			if (mapped_index == DConstants::INVALID_INDEX) {
				continue;
			}
			if (!column_names[mapped_index].empty()) {
				throw InternalException("PostHog: duplicate mapped insert column index %llu", mapped_index);
			}
			column_names[mapped_index] = column.Name();
			if (op.return_chunk) {
				return_input_index_map.push_back(mapped_index);
			}
		}
		for (idx_t i = 0; i < column_names.size(); i++) {
			if (column_names[i].empty()) {
				throw InternalException("PostHog: unmapped insert column index %llu", i);
			}
		}
	}

	auto on_conflict_do_nothing = op.on_conflict_info.action_type == OnConflictAction::NOTHING;
	string on_conflict_clause;
	if (on_conflict_do_nothing) {
		if (op.on_conflict_info.on_conflict_filter.empty()) {
			on_conflict_clause = " ON CONFLICT DO NOTHING";
		} else {
			vector<string> conflict_columns;
			for (auto &column : op.table.GetColumns().Physical()) {
				if (op.on_conflict_info.on_conflict_filter.count(column.Physical().index) == 0) {
					continue;
				}
				conflict_columns.push_back(column.Name());
			}
			if (conflict_columns.empty()) {
				throw InternalException("PostHog: ON CONFLICT filter does not map to any table columns");
			}
			on_conflict_clause = " ON CONFLICT (";
			for (idx_t i = 0; i < conflict_columns.size(); i++) {
				if (i > 0) {
					on_conflict_clause += ", ";
				}
				on_conflict_clause += KeywordHelper::WriteOptionallyQuoted(conflict_columns[i]);
			}
			on_conflict_clause += ") DO NOTHING";
		}
	}

	auto &insert =
	    planner.Make<PhysicalPostHogInsert>(op.types, *this, table.GetSchemaName(), table.name, std::move(column_names),
	                                        op.return_chunk, on_conflict_do_nothing, std::move(on_conflict_clause),
	                                        std::move(return_input_index_map), op.estimated_cardinality);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &PostHogCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	if (!IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server.");
	}

	auto &bound_info = *op.info;
	auto &base = bound_info.Base();

	// Copy the CreateTableInfo and rewrite the catalog for the remote side.
	auto copied = unique_ptr_cast<CreateInfo, CreateTableInfo>(base.Copy());
	copied->catalog = remote_catalog_;
	// Clear the query — columns have been resolved by the binder.
	copied->query.reset();

	// Build column name list from the resolved columns.
	vector<string> column_names;
	for (auto &col : copied->columns.Physical()) {
		column_names.push_back(col.Name());
	}

	auto remote_schema = copied->schema;
	auto remote_table = copied->table;
	auto &ctas = planner.Make<PhysicalPostHogCreateTableAs>(op.types, *this, std::move(copied),
	                                                        std::move(remote_schema), std::move(remote_table),
	                                                        std::move(column_names), op.estimated_cardinality);
	ctas.children.push_back(plan);
	return ctas;
}

PhysicalOperator &PostHogCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	(void)plan;
	return PlanDelete(context, planner, op);
}

PhysicalOperator &PostHogCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalDelete &op) {
	if (!IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server");
	}

	auto rewritten = RewriteRemoteDeleteSQL(context, database_name_, remote_catalog_);
	return planner.Make<PhysicalPostHogDelete>(op.types, *this, std::move(rewritten.non_returning_sql),
	                                           std::move(rewritten.returning_sql), op.return_chunk,
	                                           op.estimated_cardinality);
}

PhysicalOperator &PostHogCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                             PhysicalOperator &plan) {
	(void)plan;
	return PlanUpdate(context, planner, op);
}

PhysicalOperator &PostHogCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalUpdate &op) {
	if (!IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server.");
	}
	if (op.return_chunk) {
		throw NotImplementedException(
		    "PostHog: UPDATE ... RETURNING is not yet supported (D2: CTE wrapping rejected by remote server)");
	}

	auto rewritten = RewriteRemoteUpdateSQL(context, database_name_, remote_catalog_);
	return planner.Make<PhysicalPostHogUpdate>(op.types, *this, std::move(rewritten.non_returning_sql),
	                                           std::move(rewritten.returning_sql), op.return_chunk,
	                                           op.estimated_cardinality);
}

PhysicalOperator &PostHogCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalMergeInto &op, PhysicalOperator &plan) {
	(void)plan;
	if (!IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server.");
	}
	if (op.return_chunk) {
		// Blocked by both D2 (CTE wrapping rejected by remote server) and DuckLake
		// not supporting MERGE RETURNING. Even if D2 is fixed, DuckLake would still reject it.
		throw NotImplementedException("PostHog: MERGE ... RETURNING is not yet supported");
	}

	auto rewritten = RewriteRemoteMergeSQL(context, database_name_, remote_catalog_);
	return planner.Make<PhysicalPostHogMerge>(op.types, *this, std::move(rewritten.non_returning_sql),
	                                          std::move(rewritten.returning_sql), op.return_chunk,
	                                          op.estimated_cardinality);
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
	return false; // This is a remote database
}

string PostHogCatalog::GetDBPath() {
	return config_.flight_server;
}

void PostHogCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	if (!IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server. DROP SCHEMA failed for '%s'.",
		                       info.name.c_str());
	}

	auto remote_txn_id = PostHogTransaction::Get(context, *this).remote_txn_id;

	auto copied = info.Copy();
	copied->catalog = remote_catalog_;
	auto sql = copied->ToString();

	flight_client_->ExecuteUpdate(sql, remote_txn_id);

	std::lock_guard<std::mutex> lock(schemas_mutex_);
	schema_cache_.erase(info.name);
	schemas_loaded_ = true;
	schemas_loaded_at_ = std::chrono::steady_clock::now();
}

} // namespace duckdb
