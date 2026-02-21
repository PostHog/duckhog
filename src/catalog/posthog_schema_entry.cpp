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
#include "catalog/remote_table_function.hpp"
#include "execution/posthog_dml_rewriter.hpp"
#include "storage/posthog_transaction.hpp"
#include "utils/posthog_logger.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <arrow/c/bridge.h>

#include <cctype>
#include <iostream>

namespace duckdb {

namespace {
using SteadyClock = std::chrono::steady_clock;

int64_t ElapsedMillis(const SteadyClock::time_point &started_at) {
	return std::chrono::duration_cast<std::chrono::milliseconds>(SteadyClock::now() - started_at).count();
}

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

string QuoteIdent(const string &ident) {
	return KeywordHelper::WriteOptionallyQuoted(ident);
}

string QualifyTable(const string &catalog, const string &schema, const string &table);

void CopyKnownDefaultsByName(const ColumnList &source_columns, ColumnList &target_columns) {
	auto target_names = target_columns.GetColumnNames();
	for (auto &target_name : target_names) {
		if (!source_columns.ColumnExists(target_name)) {
			continue;
		}
		const auto &source_column = source_columns.GetColumn(target_name);
		const auto &target_column = target_columns.GetColumn(target_name);
		if (source_column.Type() != target_column.Type()) {
			continue;
		}
		if (!source_column.HasDefaultValue()) {
			continue;
		}
		target_columns.GetColumnMutable(target_name).SetDefaultValue(source_column.DefaultValue().Copy());
	}
}

string RenderSafeDefaultExpression(const ParsedExpression &expression) {
	if (expression.HasSubquery() || expression.HasParameter() || expression.IsAggregate() || expression.IsWindow()) {
		throw NotImplementedException(
		    "PostHog: only constant DEFAULT expressions are supported for remote ALTER TABLE ADD COLUMN/ADD FIELD");
	}

	switch (expression.GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
		return expression.ToString();
	case ExpressionClass::CAST: {
		auto &cast_expression = expression.Cast<CastExpression>();
		if (cast_expression.try_cast) {
			throw NotImplementedException("PostHog: TRY_CAST is not supported in DEFAULT expressions for remote ALTER "
			                              "TABLE ADD COLUMN/ADD FIELD");
		}
		return "CAST(" + RenderSafeDefaultExpression(*cast_expression.child) + " AS " +
		       cast_expression.cast_type.ToString() + ")";
	}
	default:
		throw NotImplementedException(
		    "PostHog: only constant DEFAULT expressions are supported for remote ALTER TABLE ADD COLUMN/ADD FIELD");
	}
}

string RenderColumnTypeAndDefault(const ColumnDefinition &column_definition) {
	if (column_definition.Generated()) {
		throw NotImplementedException(
		    "PostHog: generated columns are not supported for remote ALTER TABLE ADD COLUMN/ADD FIELD");
	}
	if (column_definition.Category() != TableColumnType::STANDARD) {
		throw NotImplementedException(
		    "PostHog: only standard columns are supported for remote ALTER TABLE ADD COLUMN/ADD FIELD");
	}

	string sql = " " + column_definition.Type().ToString();
	if (column_definition.HasDefaultValue()) {
		sql += " DEFAULT ";
		sql += RenderSafeDefaultExpression(column_definition.DefaultValue());
	}
	return sql;
}

string RenderAlterTablePrefix(const AlterTableInfo &info) {
	string sql = "ALTER TABLE";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		sql += " IF EXISTS";
	}
	sql += " " + QualifyTable(info.catalog, info.schema, info.name);
	return sql;
}

string RenderAddColumnSQL(const AddColumnInfo &info) {
	string sql = RenderAlterTablePrefix(info);
	sql += " ADD COLUMN";
	if (info.if_column_not_exists) {
		sql += " IF NOT EXISTS";
	}
	sql += " " + QuoteIdent(info.new_column.Name());
	sql += RenderColumnTypeAndDefault(info.new_column);
	sql += ";";
	return sql;
}

string RenderAddFieldSQL(const AddFieldInfo &info) {
	if (info.column_path.empty()) {
		throw InternalException("PostHog: ADD FIELD requires a non-empty column path");
	}

	string sql = RenderAlterTablePrefix(info);
	sql += " ADD COLUMN";
	if (info.if_field_not_exists) {
		sql += " IF NOT EXISTS";
	}
	sql += " ";
	for (idx_t i = 0; i < info.column_path.size(); i++) {
		if (i > 0) {
			sql += ".";
		}
		sql += QuoteIdent(info.column_path[i]);
	}
	sql += ".";
	sql += QuoteIdent(info.new_field.Name());
	sql += RenderColumnTypeAndDefault(info.new_field);
	sql += ";";
	return sql;
}

string RenderRenameTableSQL(const RenameTableInfo &info) {
	string sql = RenderAlterTablePrefix(info);
	sql += " RENAME TO ";
	sql += QuoteIdent(info.new_table_name);
	sql += ";";
	return sql;
}

string RenderAlterTableSQL(const AlterInfo &info) {
	auto &alter_table_info = info.Cast<AlterTableInfo>();
	switch (alter_table_info.alter_table_type) {
	case AlterTableType::ADD_COLUMN:
		return RenderAddColumnSQL(info.Cast<AddColumnInfo>());
	case AlterTableType::ADD_FIELD:
		return RenderAddFieldSQL(info.Cast<AddFieldInfo>());
	case AlterTableType::RENAME_TABLE:
		return RenderRenameTableSQL(info.Cast<RenameTableInfo>());
	default:
		return info.ToString();
	}
}

string QualifyTable(const string &catalog, const string &schema, const string &table) {
	if (catalog.empty()) {
		return QuoteIdent(schema) + "." + QuoteIdent(table);
	}
	return QuoteIdent(catalog) + "." + QuoteIdent(schema) + "." + QuoteIdent(table);
}

} // namespace

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
	if (!posthog_catalog_.IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server.");
	}

	if (info.query) {
		throw NotImplementedException("PostHog: CREATE TABLE AS SELECT is not supported for remote databases");
	}

	auto &context = transaction.GetContext();
	auto remote_txn_id = PostHogTransaction::Get(context, posthog_catalog_).remote_txn_id;

	const auto &remote_catalog = posthog_catalog_.GetRemoteCatalog();
	auto copied = info.Base().Copy();
	auto &remote_info = copied->Cast<CreateTableInfo>();
	remote_info.catalog = remote_catalog;
	auto sql = remote_info.ToString();

	auto &client = posthog_catalog_.GetFlightClient();
	client.ExecuteUpdate(sql, remote_txn_id);

	// Refresh local table entry using query schema inference (works for uncommitted DDL in the same txn).
	auto qualified = QualifyTable(remote_catalog, name, remote_info.table);
	auto arrow_schema = client.GetQuerySchema("SELECT * FROM " + qualified, remote_txn_id);

	vector<string> column_names;
	vector<LogicalType> column_types;
	PopulateTableSchemaFromArrow(DBConfig::GetConfig(posthog_catalog_.GetDatabase()), arrow_schema, column_names,
	                             column_types);

	auto create_info = make_uniq<CreateTableInfo>(*this, remote_info.table);
	for (idx_t i = 0; i < column_names.size(); i++) {
		create_info->columns.AddColumn(ColumnDefinition(column_names[i], column_types[i]));
	}
	CopyKnownDefaultsByName(remote_info.columns, create_info->columns);
	create_info->columns.Finalize();

	std::lock_guard<std::mutex> lock(tables_mutex_);
	auto table_entry =
	    make_uniq<PostHogTableEntry>(catalog, *this, *create_info, posthog_catalog_, std::move(arrow_schema));
	auto *result = table_entry.get();
	table_cache_[remote_info.table] = std::move(table_entry);
	tables_loaded_ = true;
	tables_loaded_at_ = std::chrono::steady_clock::now();
	return result;
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                              CreateFunctionInfo &info) {
	throw NotImplementedException("PostHog: CREATE FUNCTION not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
	throw NotImplementedException("PostHog: CREATE INDEX not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (!posthog_catalog_.IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server.");
	}

	auto &context = transaction.GetContext();
	auto remote_txn_id = PostHogTransaction::Get(context, posthog_catalog_).remote_txn_id;

	auto sql = BuildRemoteCreateViewSQL(info, posthog_catalog_.GetDatabaseName(), posthog_catalog_.GetRemoteCatalog());

	posthog_catalog_.GetFlightClient().ExecuteUpdate(sql, remote_txn_id);
	return nullptr;
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                              CreateSequenceInfo &info) {
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

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                               CreateCollationInfo &info) {
	throw NotImplementedException("PostHog: CREATE COLLATION not supported on remote database");
}

optional_ptr<CatalogEntry> PostHogSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("PostHog: CREATE TYPE not supported on remote database");
}

//===----------------------------------------------------------------------===//
// Alter/Drop Operations
//===----------------------------------------------------------------------===//

void PostHogSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (!posthog_catalog_.IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server.");
	}

	if (info.type != AlterType::ALTER_TABLE) {
		throw NotImplementedException("PostHog: only ALTER TABLE is supported for remote databases");
	}

	auto &context = transaction.GetContext();
	auto remote_txn_id = PostHogTransaction::Get(context, posthog_catalog_).remote_txn_id;

	const auto &remote_catalog = posthog_catalog_.GetRemoteCatalog();
	auto copied = info.Copy();
	copied->catalog = remote_catalog;
	auto sql = RenderAlterTableSQL(*copied);

	auto &client = posthog_catalog_.GetFlightClient();
	client.ExecuteUpdate(sql, remote_txn_id);

	// For RENAME, the table now lives at the new name on the remote server.
	auto &alter_table_info = info.Cast<AlterTableInfo>();
	string effective_table_name = info.name;
	if (alter_table_info.alter_table_type == AlterTableType::RENAME_TABLE) {
		effective_table_name = info.Cast<RenameTableInfo>().new_table_name;
	}

	auto qualified = QualifyTable(remote_catalog, name, effective_table_name);
	auto arrow_schema = client.GetQuerySchema("SELECT * FROM " + qualified, remote_txn_id);

	vector<string> column_names;
	vector<LogicalType> column_types;
	PopulateTableSchemaFromArrow(DBConfig::GetConfig(posthog_catalog_.GetDatabase()), arrow_schema, column_names,
	                             column_types);

	auto create_info = make_uniq<CreateTableInfo>(*this, effective_table_name);
	for (idx_t i = 0; i < column_names.size(); i++) {
		create_info->columns.AddColumn(ColumnDefinition(column_names[i], column_types[i]));
	}
	std::lock_guard<std::mutex> lock(tables_mutex_);
	auto existing_entry = table_cache_.find(info.name);
	if (existing_entry != table_cache_.end()) {
		CopyKnownDefaultsByName(existing_entry->second->GetColumns(), create_info->columns);
	}
	if (alter_table_info.alter_table_type == AlterTableType::ADD_COLUMN) {
		auto &new_column = info.Cast<AddColumnInfo>().new_column;
		if (new_column.HasDefaultValue() && create_info->columns.ColumnExists(new_column.Name())) {
			const auto &hydrated_column = create_info->columns.GetColumn(new_column.Name());
			if (hydrated_column.Type() == new_column.Type()) {
				create_info->columns.GetColumnMutable(new_column.Name())
				    .SetDefaultValue(new_column.DefaultValue().Copy());
			}
		}
	}
	create_info->columns.Finalize();

	table_cache_.erase(info.name);
	table_cache_[effective_table_name] =
	    make_uniq<PostHogTableEntry>(catalog, *this, *create_info, posthog_catalog_, std::move(arrow_schema));
	tables_loaded_ = true;
	tables_loaded_at_ = std::chrono::steady_clock::now();
}

void PostHogSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (!posthog_catalog_.IsConnected()) {
		throw CatalogException("PostHog: Not connected to remote server.");
	}

	if (info.type != CatalogType::TABLE_ENTRY && info.type != CatalogType::VIEW_ENTRY) {
		throw NotImplementedException("PostHog: only DROP TABLE and DROP VIEW are supported for remote databases");
	}

	auto remote_txn_id = PostHogTransaction::Get(context, posthog_catalog_).remote_txn_id;

	const auto &remote_catalog = posthog_catalog_.GetRemoteCatalog();
	auto copied = info.Copy();
	copied->catalog = remote_catalog;
	auto sql = copied->ToString();

	posthog_catalog_.GetFlightClient().ExecuteUpdate(sql, remote_txn_id);

	if (info.type == CatalogType::TABLE_ENTRY) {
		std::lock_guard<std::mutex> lock(tables_mutex_);
		table_cache_.erase(info.name);
		tables_loaded_ = true;
		tables_loaded_at_ = std::chrono::steady_clock::now();
	}
}

//===----------------------------------------------------------------------===//
// Table Loading (Lazy)
//===----------------------------------------------------------------------===//

void PostHogSchemaEntry::LoadTablesIfNeeded() {
	// Note: This method is called while holding tables_mutex_ from the caller
	// or should be called with the lock already held
	auto op_started_at = SteadyClock::now();

	// Check if cache is still valid
	if (tables_loaded_) {
		auto now = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - tables_loaded_at_).count();
		if (elapsed < CACHE_TTL_SECONDS) {
			return; // Cache is still valid
		}
		// Cache expired, need to refresh
		std::cerr << "[PostHog] Table cache expired for schema " << name << ", refreshing..." << '\n';
	}

	if (!posthog_catalog_.IsConnected()) {
		std::cerr << "[PostHog] Cannot load tables: not connected" << '\n';
		return;
	}

	try {
		const auto &remote_catalog = posthog_catalog_.GetRemoteCatalog();
		std::cerr << "[PostHog] Loading tables for catalog." << remote_catalog << ".schema." << name << '\n';
		auto &client = posthog_catalog_.GetFlightClient();
		auto list_tables_started_at = SteadyClock::now();
		auto table_names = client.ListTables(remote_catalog, name);
		POSTHOG_LOG_DEBUG("Schema '%s': ListTables returned %zu tables in %lld ms", name.c_str(), table_names.size(),
		                  static_cast<long long>(ElapsedMillis(list_tables_started_at)));

		unordered_set<string> remote_tables;
		remote_tables.reserve(table_names.size());
		for (const auto &t : table_names) {
			remote_tables.insert(t);
		}

		// Prune tables that no longer exist remotely.
		for (auto it = table_cache_.begin(); it != table_cache_.end();) {
			if (remote_tables.find(it->first) == remote_tables.end()) {
				it = table_cache_.erase(it);
				continue;
			}
			++it;
		}

		// Create entries for tables not already in cache
		size_t created_count = 0;
		for (const auto &table_name : table_names) {
			if (table_cache_.find(table_name) == table_cache_.end()) {
				POSTHOG_LOG_DEBUG("Schema '%s': hydrating table '%s'", name.c_str(), table_name.c_str());
				CreateTableEntry(table_name);
				created_count++;
			}
		}

		tables_loaded_ = true;
		tables_loaded_at_ = std::chrono::steady_clock::now();
		std::cerr << "[PostHog] Loaded " << table_names.size() << " tables for schema " << name << '\n';
		POSTHOG_LOG_DEBUG("Schema '%s': table load complete (created=%zu cached=%zu total_ms=%lld)", name.c_str(),
		                  created_count, table_cache_.size(), static_cast<long long>(ElapsedMillis(op_started_at)));

	} catch (const std::exception &e) {
		std::cerr << "[PostHog] Failed to load tables for schema " << name << ": " << e.what() << '\n';
		if (IsConnectionFailureMessage(e.what())) {
			throw CatalogException("PostHog: Not connected to remote server.");
		}
	}
}

void PostHogSchemaEntry::CreateTableEntry(const string &table_name) {
	// Note: Called with tables_mutex_ already held
	auto op_started_at = SteadyClock::now();
	POSTHOG_LOG_DEBUG("Schema '%s': CreateTableEntry start table='%s'", name.c_str(), table_name.c_str());

	if (!posthog_catalog_.IsConnected()) {
		return;
	}

	// Skip if already exists
	if (table_cache_.find(table_name) != table_cache_.end()) {
		return;
	}

	try {
		auto &client = posthog_catalog_.GetFlightClient();
		const auto &remote_catalog = posthog_catalog_.GetRemoteCatalog();
		auto schema_started_at = SteadyClock::now();
		auto arrow_schema = client.GetTableSchema(remote_catalog, name, table_name);
		POSTHOG_LOG_DEBUG("Schema '%s': GetTableSchema('%s') completed in %lld ms", name.c_str(), table_name.c_str(),
		                  static_cast<long long>(ElapsedMillis(schema_started_at)));

		vector<string> column_names;
		vector<LogicalType> column_types;
		PopulateTableSchemaFromArrow(DBConfig::GetConfig(posthog_catalog_.GetDatabase()), arrow_schema, column_names,
		                             column_types);

		auto create_info = make_uniq<CreateTableInfo>(*this, table_name);
		for (idx_t i = 0; i < column_names.size(); i++) {
			create_info->columns.AddColumn(ColumnDefinition(column_names[i], column_types[i]));
		}
		create_info->columns.Finalize();

		auto table_entry = make_uniq<PostHogTableEntry>(catalog, *this, *create_info, posthog_catalog_, arrow_schema);
		table_cache_.emplace(table_name, std::move(table_entry));
		POSTHOG_LOG_DEBUG("Schema '%s': CreateTableEntry done table='%s' total_ms=%lld", name.c_str(),
		                  table_name.c_str(), static_cast<long long>(ElapsedMillis(op_started_at)));
	} catch (const std::exception &e) {
		POSTHOG_LOG_DEBUG("Table metadata hydration skipped for '%s.%s': %s", name.c_str(), table_name.c_str(),
		                  e.what());
		if (IsConnectionFailureMessage(e.what())) {
			throw CatalogException("PostHog: Not connected to remote server.");
		}
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
	// Proxy known DuckLake catalog-scoped table functions through Flight SQL.
	// Only allowlisted names are proxied; unknown names return nullptr so that
	// DuckDB's binder doesn't confuse an unknown scalar function (e.g. a typo)
	// with a table function we speculatively created.
	// The sentinel test in ducklake_table_functions_conformance.test verifies
	// this list stays in sync with DuckLake's actual function inventory.
	if (lookup_info.GetCatalogType() == CatalogType::TABLE_FUNCTION_ENTRY) {
		static const unordered_set<string> known_table_functions = {"add_data_files",
		                                                            "cleanup_old_files",
		                                                            "current_snapshot",
		                                                            "delete_orphaned_files",
		                                                            "expire_snapshots",
		                                                            "flush_inlined_data",
		                                                            "last_committed_snapshot",
		                                                            "list_files",
		                                                            "merge_adjacent_files",
		                                                            "options",
		                                                            "rewrite_data_files",
		                                                            "set_commit_message",
		                                                            "set_option",
		                                                            "snapshots",
		                                                            "table_changes",
		                                                            "table_deletions",
		                                                            "table_info",
		                                                            "table_insertions"};
		auto &name = lookup_info.GetEntryName();
		if (known_table_functions.find(name) == known_table_functions.end()) {
			return nullptr;
		}
		auto entry = CreateRemoteTableFunctionEntry(posthog_catalog_, *this, name);
		auto *result = entry.get();
		table_function_cache_[name] = std::move(entry);
		return result;
	}

	// VIEW_ENTRY is required here: DuckDB resolves DROP VIEW (and other view operations)
	// by looking up the entry as VIEW_ENTRY. Views are stored in table_cache_ because the
	// remote server's ListTables returns both tables and views indistinguishably.
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY &&
	    lookup_info.GetCatalogType() != CatalogType::VIEW_ENTRY) {
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
