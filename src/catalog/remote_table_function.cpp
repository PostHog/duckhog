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
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {

//===----------------------------------------------------------------------===//
// TableFunctionInfo subclass: carries the catalog ref and remote SQL
//===----------------------------------------------------------------------===//

struct RemoteTableFunctionInfo : public TableFunctionInfo {
	RemoteTableFunctionInfo(PostHogCatalog &catalog_p, string function_base_p)
	    : catalog(catalog_p), function_base(std::move(function_base_p)) {
	}

	PostHogCatalog &catalog;
	// The remote function base, e.g. "ducklake"."snapshots" (without trailing parens/args).
	string function_base;
};

//===----------------------------------------------------------------------===//
// Bind data: extends ArrowScanFunctionData for Arrow stream consumption
//===----------------------------------------------------------------------===//

struct RemoteTableFunctionBindData : public ArrowScanFunctionData {
	RemoteTableFunctionBindData(PostHogCatalog &catalog_p, string function_ref_p);

	PostHogCatalog &catalog;
	// The remote function reference, e.g. "ducklake"."snapshots"()
	string function_ref;

	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr, ArrowStreamParameters &parameters);
};

struct RemoteTableFunctionStreamFactory {
	const RemoteTableFunctionBindData *bind_data;
	std::optional<TransactionId> txn_id;
};

RemoteTableFunctionBindData::RemoteTableFunctionBindData(PostHogCatalog &catalog_p, string function_ref_p)
    : ArrowScanFunctionData(&RemoteTableFunctionBindData::Produce, reinterpret_cast<uintptr_t>(this)),
      catalog(catalog_p), function_ref(std::move(function_ref_p)) {
}

unique_ptr<ArrowArrayStreamWrapper> RemoteTableFunctionBindData::Produce(uintptr_t factory_ptr,
                                                                         ArrowStreamParameters &parameters) {
	auto *factory = reinterpret_cast<RemoteTableFunctionStreamFactory *>(factory_ptr);
	auto *bind_data = factory->bind_data;

	// Build projected column list from the parameters, mirroring PostHogArrowStream::Produce.
	auto &columns = parameters.projected_columns.columns;
	string columns_str;
	if (columns.empty()) {
		columns_str = "*";
	} else {
		for (size_t i = 0; i < columns.size(); i++) {
			if (i > 0) {
				columns_str += ", ";
			}
			columns_str += "\"" + columns[i] + "\"";
		}
	}
	string query = "SELECT " + columns_str + " FROM " + bind_data->function_ref;

	auto stream_state = std::make_shared<PostHogArrowStreamState>(bind_data->catalog, query, factory->txn_id);

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

// Build the full function_ref for a zero-arg call, e.g. "ducklake"."snapshots"().
static string BuildZeroArgRef(const string &function_base) {
	return function_base + "()";
}

// Build the function_ref for table_changes(table_name, start_snapshot, end_snapshot).
// Escapes the VARCHAR argument; BIGINT arguments are rendered unquoted.
static string BuildTableChangesRef(const string &function_base, const vector<Value> &inputs) {
	auto table_name = StringUtil::Replace(inputs[0].ToString(), "'", "''");
	auto start_snap = inputs[1].ToString();
	auto end_snap = inputs[2].ToString();
	return function_base + "('" + table_name + "', " + start_snap + ", " + end_snap + ")";
}

static unique_ptr<FunctionData> RemoteTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto &fn_info = input.info->Cast<RemoteTableFunctionInfo>();
	auto &catalog = fn_info.catalog;
	auto function_ref = BuildZeroArgRef(fn_info.function_base);

	auto bind_data = make_uniq<RemoteTableFunctionBindData>(catalog, function_ref);

	// Discover the return schema by probing the remote server with SELECT *.
	string schema_query = "SELECT * FROM " + function_ref;
	std::optional<TransactionId> remote_txn_id;
	try {
		remote_txn_id = PostHogTransaction::Get(context, catalog).remote_txn_id;
	} catch (const std::exception &) {
		remote_txn_id = std::nullopt;
	}

	auto arrow_schema = catalog.GetFlightClient().GetQuerySchema(schema_query, remote_txn_id);

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

// Bind callback for parameterized table functions (e.g. table_changes(VARCHAR, BIGINT, BIGINT)).
// Reads positional arguments and interpolates them into the remote function call.
static unique_ptr<FunctionData> RemoteTableChangesBindArgs(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto &fn_info = input.info->Cast<RemoteTableFunctionInfo>();
	auto &catalog = fn_info.catalog;
	auto function_ref = BuildTableChangesRef(fn_info.function_base, input.inputs);

	auto bind_data = make_uniq<RemoteTableFunctionBindData>(catalog, function_ref);

	string schema_query = "SELECT * FROM " + function_ref;
	std::optional<TransactionId> remote_txn_id;
	try {
		remote_txn_id = PostHogTransaction::Get(context, catalog).remote_txn_id;
	} catch (const std::exception &) {
		remote_txn_id = std::nullopt;
	}

	auto arrow_schema = catalog.GetFlightClient().GetQuerySchema(schema_query, remote_txn_id);

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
	string function_base;
	if (remote_catalog.empty()) {
		function_base = "\"" + function_name + "\"";
	} else {
		function_base = "\"" + remote_catalog + "\".\"" + function_name + "\"";
	}

	auto fn_info = make_shared_ptr<RemoteTableFunctionInfo>(catalog, function_base);

	// Zero-arg overload (e.g. snapshots(), table_info(), table_insertions()).
	TableFunction zero_arg(function_name, {}, RemoteTableFunctionExecute, RemoteTableFunctionBind,
	                       RemoteTableFunctionInitGlobal, RemoteTableFunctionInitLocal);
	zero_arg.projection_pushdown = true;
	zero_arg.filter_pushdown = false;
	zero_arg.function_info = fn_info;

	TableFunctionSet func_set(function_name);
	func_set.AddFunction(std::move(zero_arg));

	// table_changes(table_name VARCHAR, start_snapshot BIGINT, end_snapshot BIGINT).
	// Hardcoded: this is the only parameterized catalog-scoped table function in DuckLake.
	// The sentinel test in ducklake_table_functions_conformance.test will fail if this changes.
	if (function_name == "table_changes") {
		TableFunction with_args(function_name, {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
		                        RemoteTableFunctionExecute, RemoteTableChangesBindArgs, RemoteTableFunctionInitGlobal,
		                        RemoteTableFunctionInitLocal);
		with_args.projection_pushdown = true;
		with_args.filter_pushdown = false;
		with_args.function_info = fn_info;
		func_set.AddFunction(std::move(with_args));
	}

	auto info = make_uniq<CreateTableFunctionInfo>(std::move(func_set));
	info->name = function_name;

	return make_uniq<TableFunctionCatalogEntry>(catalog, schema, *info);
}

} // namespace duckdb
