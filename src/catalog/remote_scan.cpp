//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/remote_scan.cpp
//
// Table function implementation for executing remote queries via Flight SQL
//===----------------------------------------------------------------------===//

#include "catalog/remote_scan.hpp"
#include "catalog/posthog_catalog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind Data
//===----------------------------------------------------------------------===//

PostHogRemoteScanBindData::PostHogRemoteScanBindData(PostHogCatalog &catalog_p, const string &schema_name_p,
                                                     const string &table_name_p)
    : ArrowScanFunctionData(&PostHogArrowStream::Produce, reinterpret_cast<uintptr_t>(&arrow_stream)),
      catalog(catalog_p), schema_name(schema_name_p), table_name(table_name_p) {
    projection_pushdown_enabled = false;
    arrow_stream.get_schema = nullptr;
    arrow_stream.get_next = nullptr;
    arrow_stream.get_last_error = nullptr;
    arrow_stream.release = nullptr;
    arrow_stream.private_data = nullptr;
}

PostHogRemoteScanBindData::~PostHogRemoteScanBindData() {
    if (arrow_stream.release) {
        arrow_stream.release(&arrow_stream);
    }
}

//===----------------------------------------------------------------------===//
// Bind Function
//===----------------------------------------------------------------------===//

unique_ptr<FunctionData> PostHogRemoteScan::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
    // This bind function is used for direct table function calls
    // For table scans, we use CreateBindData instead
    throw NotImplementedException("PostHog remote_scan should not be called directly");
}

unique_ptr<FunctionData> PostHogRemoteScan::CreateBindData(PostHogCatalog &catalog, const string &schema_name,
                                                           const string &table_name,
                                                           const vector<string> &column_names,
                                                           const vector<LogicalType> &column_types) {
    auto bind_data = make_uniq<PostHogRemoteScanBindData>(catalog, schema_name, table_name);

    bind_data->column_names = column_names;
    bind_data->column_types = column_types;

    // Build the SELECT query
    // TODO: Support column projection pushdown
    string columns_str;
    if (column_names.empty()) {
        columns_str = "*";
    } else {
        for (size_t i = 0; i < column_names.size(); i++) {
            if (i > 0) {
                columns_str += ", ";
            }
            // Quote column names to handle special characters
            columns_str += "\"" + column_names[i] + "\"";
        }
    }

    // Build the full query
    // Quote schema and table names
    bind_data->query =
        "SELECT " + columns_str + " FROM \"" + schema_name + "\".\"" + table_name + "\"";

    bind_data->stream_state = std::make_shared<PostHogArrowStreamState>(catalog, bind_data->query);
    PostHogArrowStream::Initialize(bind_data->arrow_stream, bind_data->stream_state);

    PostHogArrowStream::GetSchema(&bind_data->arrow_stream, bind_data->schema_root.arrow_schema);
    ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(catalog.GetDatabase()), bind_data->arrow_table,
                                                 bind_data->schema_root.arrow_schema);
    bind_data->all_types = bind_data->arrow_table.GetTypes();

    return bind_data;
}

//===----------------------------------------------------------------------===//
// Init Functions
//===----------------------------------------------------------------------===//

unique_ptr<GlobalTableFunctionState> PostHogRemoteScan::InitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    return ArrowTableFunction::ArrowScanInitGlobal(context, input);
}

unique_ptr<LocalTableFunctionState> PostHogRemoteScan::InitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
    return ArrowTableFunction::ArrowScanInitLocal(context, input, global_state);
}

//===----------------------------------------------------------------------===//
// Execute Function
//===----------------------------------------------------------------------===//

void PostHogRemoteScan::Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    ArrowTableFunction::ArrowScanFunction(context, data, output);
}

//===----------------------------------------------------------------------===//
// Progress Function
//===----------------------------------------------------------------------===//

double PostHogRemoteScan::Progress(ClientContext &context, const FunctionData *bind_data,
                                    const GlobalTableFunctionState *global_state) {
    (void)context;
    (void)bind_data;
    (void)global_state;
    return 0.0;
}

//===----------------------------------------------------------------------===//
// Get Table Function
//===----------------------------------------------------------------------===//

TableFunction PostHogRemoteScan::GetFunction() {
    TableFunction func("posthog_remote_scan", {}, Execute, Bind, InitGlobal, InitLocal);
    // We currently fetch all columns from Flight; disable projection pushdown to keep
    // Arrow array column order aligned with DuckDB's expected indices.
    func.projection_pushdown = false;
    func.filter_pushdown = false;     // TODO: Implement filter pushdown
    func.table_scan_progress = Progress;
    return func;
}

} // namespace duckdb
