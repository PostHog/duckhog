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
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind Data
//===----------------------------------------------------------------------===//

PostHogRemoteScanBindData::PostHogRemoteScanBindData(PostHogCatalog &catalog_p, const string &schema_name_p,
                                                     const string &table_name_p)
    : catalog(catalog_p), schema_name(schema_name_p), table_name(table_name_p) {
}

//===----------------------------------------------------------------------===//
// Global State
//===----------------------------------------------------------------------===//

PostHogRemoteScanGlobalState::PostHogRemoteScanGlobalState() : current_row(0), executed(false) {
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

    return bind_data;
}

//===----------------------------------------------------------------------===//
// Init Functions
//===----------------------------------------------------------------------===//

unique_ptr<GlobalTableFunctionState> PostHogRemoteScan::InitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    return make_uniq<PostHogRemoteScanGlobalState>();
}

unique_ptr<LocalTableFunctionState> PostHogRemoteScan::InitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
    return make_uniq<PostHogRemoteScanLocalState>();
}

//===----------------------------------------------------------------------===//
// Execute Function
//===----------------------------------------------------------------------===//

void PostHogRemoteScan::Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    (void)context;
    (void)output;

    auto &bind_data = data.bind_data->Cast<PostHogRemoteScanBindData>();
    throw NotImplementedException("PostHog: remote scan disabled during Arrow conversion refactor for " +
                                  bind_data.schema_name + "." + bind_data.table_name);
}

//===----------------------------------------------------------------------===//
// Progress Function
//===----------------------------------------------------------------------===//

double PostHogRemoteScan::Progress(ClientContext &context, const FunctionData *bind_data,
                                    const GlobalTableFunctionState *global_state) {
    auto &state = global_state->Cast<PostHogRemoteScanGlobalState>();

    if (!state.executed || !state.result_table) {
        return 0.0;
    }

    if (state.result_table->num_rows() == 0) {
        return 100.0;
    }

    return 100.0 * static_cast<double>(state.current_row) / static_cast<double>(state.result_table->num_rows());
}

//===----------------------------------------------------------------------===//
// Get Table Function
//===----------------------------------------------------------------------===//

TableFunction PostHogRemoteScan::GetFunction() {
    TableFunction func("posthog_remote_scan", {}, Execute, Bind, InitGlobal, InitLocal);
    func.projection_pushdown = true;  // Required for virtual tables
    func.filter_pushdown = false;     // TODO: Implement filter pushdown
    func.table_scan_progress = Progress;
    return func;
}

} // namespace duckdb
