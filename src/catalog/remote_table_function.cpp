//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/remote_table_function.cpp
//
// Proxy catalog-level table functions (e.g. snapshots()) through Flight SQL
//===----------------------------------------------------------------------===//

#include "catalog/remote_table_function.hpp"
#include "catalog/posthog_catalog.hpp"
#include "flight/arrow_stream.hpp"
#include "storage/posthog_transaction.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {

//===----------------------------------------------------------------------===//
// TableFunctionInfo subclass: carries the catalog ref and remote SQL
//===----------------------------------------------------------------------===//

struct RemoteTableFunctionInfo : public TableFunctionInfo {
	RemoteTableFunctionInfo(PostHogCatalog &catalog_p, string remote_sql_p)
	    : catalog(catalog_p), remote_sql(std::move(remote_sql_p)) {
	}

	PostHogCatalog &catalog;
	string remote_sql;
};

//===----------------------------------------------------------------------===//
// Bind data: extends ArrowScanFunctionData for Arrow stream consumption
//===----------------------------------------------------------------------===//

struct RemoteTableFunctionBindData : public ArrowScanFunctionData {
	RemoteTableFunctionBindData(PostHogCatalog &catalog_p, string remote_sql_p);

	PostHogCatalog &catalog;
	string remote_sql;

	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr, ArrowStreamParameters &parameters);
};

struct RemoteTableFunctionStreamFactory {
	const RemoteTableFunctionBindData *bind_data;
	std::optional<TransactionId> txn_id;
};

RemoteTableFunctionBindData::RemoteTableFunctionBindData(PostHogCatalog &catalog_p, string remote_sql_p)
    : ArrowScanFunctionData(&RemoteTableFunctionBindData::Produce, reinterpret_cast<uintptr_t>(this)),
      catalog(catalog_p), remote_sql(std::move(remote_sql_p)) {
}

unique_ptr<ArrowArrayStreamWrapper> RemoteTableFunctionBindData::Produce(uintptr_t factory_ptr,
                                                                         ArrowStreamParameters & /*parameters*/) {
	auto *factory = reinterpret_cast<RemoteTableFunctionStreamFactory *>(factory_ptr);
	auto *bind_data = factory->bind_data;

	auto stream_state =
	    std::make_shared<PostHogArrowStreamState>(bind_data->catalog, bind_data->remote_sql, factory->txn_id);

	ArrowArrayStream tmp_stream;
	PostHogArrowStream::Initialize(tmp_stream, std::move(stream_state));

	auto res = make_uniq<ArrowArrayStreamWrapper>();
	res->arrow_array_stream = tmp_stream;

	tmp_stream.release = nullptr;
	tmp_stream.get_schema = nullptr;
	tmp_stream.get_next = nullptr;
	tmp_stream.get_last_error = nullptr;
	tmp_stream.private_data = nullptr;

	return res;
}

//===----------------------------------------------------------------------===//
// Global state
//===----------------------------------------------------------------------===//

struct RemoteTableFunctionGlobalState : public ArrowScanGlobalState {
	unique_ptr<RemoteTableFunctionStreamFactory> stream_factory;
};

//===----------------------------------------------------------------------===//
// Table function callbacks
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> RemoteTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto &fn_info = input.info->Cast<RemoteTableFunctionInfo>();
	auto &catalog = fn_info.catalog;
	auto &remote_sql = fn_info.remote_sql;

	auto bind_data = make_uniq<RemoteTableFunctionBindData>(catalog, remote_sql);

	// Discover the return schema by probing the remote server.
	std::optional<TransactionId> remote_txn_id;
	try {
		remote_txn_id = PostHogTransaction::Get(context, catalog).remote_txn_id;
	} catch (const std::exception &) {
		remote_txn_id = std::nullopt;
	}

	auto arrow_schema = catalog.GetFlightClient().GetQuerySchema(remote_sql, remote_txn_id);

	auto status = arrow::ExportSchema(*arrow_schema, &bind_data->schema_root.arrow_schema);
	if (!status.ok()) {
		throw IOException("PostHog: Failed to export Arrow schema for remote table function: " + status.ToString());
	}

	ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), bind_data->arrow_table,
	                                             bind_data->schema_root.arrow_schema);
	names = bind_data->arrow_table.GetNames();
	return_types = bind_data->arrow_table.GetTypes();
	bind_data->all_types = return_types;

	return bind_data;
}

static unique_ptr<GlobalTableFunctionState> RemoteTableFunctionInitGlobal(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RemoteTableFunctionBindData>();

	std::optional<TransactionId> remote_txn_id;
	try {
		remote_txn_id = PostHogTransaction::Get(context, bind_data.catalog).remote_txn_id;
	} catch (const std::exception &) {
		remote_txn_id = std::nullopt;
	}

	ArrowStreamParameters parameters;
	auto &arrow_types = bind_data.arrow_table.GetColumns();
	for (idx_t idx = 0; idx < input.column_ids.size(); idx++) {
		auto col_idx = input.column_ids[idx];
		if (col_idx != COLUMN_IDENTIFIER_ROW_ID) {
			auto &schema_c = *bind_data.schema_root.arrow_schema.children[col_idx];
			arrow_types.at(col_idx)->ThrowIfInvalid();
			parameters.projected_columns.projection_map[idx] = schema_c.name;
			parameters.projected_columns.columns.emplace_back(schema_c.name);
			parameters.projected_columns.filter_to_col[idx] = col_idx;
		}
	}

	auto result = make_uniq<RemoteTableFunctionGlobalState>();
	result->stream_factory = make_uniq<RemoteTableFunctionStreamFactory>();
	result->stream_factory->bind_data = &bind_data;
	result->stream_factory->txn_id = std::move(remote_txn_id);
	result->stream = bind_data.scanner_producer(reinterpret_cast<uintptr_t>(result->stream_factory.get()), parameters);

	result->max_threads = context.db->NumberOfThreads();
	if (!input.projection_ids.empty()) {
		result->projection_ids = input.projection_ids;
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				result->scanned_types.emplace_back(LogicalType(LogicalType::ROW_TYPE));
			} else {
				result->scanned_types.push_back(bind_data.all_types[col_idx]);
			}
		}
	}
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> RemoteTableFunctionInitLocal(ExecutionContext &context,
                                                                        TableFunctionInitInput &input,
                                                                        GlobalTableFunctionState *global_state) {
	return ArrowTableFunction::ArrowScanInitLocal(context, input, global_state);
}

static void RemoteTableFunctionExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ArrowTableFunction::ArrowScanFunction(context, data, output);
}

//===----------------------------------------------------------------------===//
// Factory: create the catalog entry
//===----------------------------------------------------------------------===//

unique_ptr<TableFunctionCatalogEntry>
CreateRemoteTableFunctionEntry(PostHogCatalog &catalog, SchemaCatalogEntry &schema, const string &function_name) {
	const auto &remote_catalog = catalog.GetRemoteCatalog();
	string remote_sql;
	if (remote_catalog.empty()) {
		remote_sql = "SELECT * FROM \"" + function_name + "\"()";
	} else {
		remote_sql = "SELECT * FROM \"" + remote_catalog + "\".\"" + function_name + "\"()";
	}

	// Create the no-arg table function with callbacks.
	TableFunction func(function_name, {}, RemoteTableFunctionExecute, RemoteTableFunctionBind,
	                   RemoteTableFunctionInitGlobal, RemoteTableFunctionInitLocal);
	func.projection_pushdown = true;
	func.filter_pushdown = false;
	func.function_info = make_shared_ptr<RemoteTableFunctionInfo>(catalog, remote_sql);

	auto info = make_uniq<CreateTableFunctionInfo>(std::move(func));
	info->name = function_name;

	return make_uniq<TableFunctionCatalogEntry>(catalog, schema, *info);
}

} // namespace duckdb
