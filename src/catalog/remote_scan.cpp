//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/remote_scan.cpp
//
// Table function implementation for executing remote queries via Flight SQL
//===----------------------------------------------------------------------===//

#include "catalog/remote_scan.hpp"
#include "catalog/posthog_catalog.hpp"
#include "flight/arrow_conversion.hpp"
#include "utils/posthog_logger.hpp"

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
    auto &bind_data = data.bind_data->Cast<PostHogRemoteScanBindData>();
    auto &global_state = data.global_state->Cast<PostHogRemoteScanGlobalState>();

    // Execute the remote query if we haven't already
    if (!global_state.executed) {
        if (!bind_data.catalog.IsConnected()) {
            throw ConnectionException("PostHog: Not connected to remote server");
        }

        try {
            POSTHOG_LOG_DEBUG("Executing remote query: %s", bind_data.query.c_str());
            auto &client = bind_data.catalog.GetFlightClient();
            global_state.result_table = client.ExecuteQuery(bind_data.query);
            global_state.executed = true;
            POSTHOG_LOG_DEBUG("Query returned %lld rows", global_state.result_table->num_rows());
        } catch (const std::exception &e) {
            POSTHOG_LOG_ERROR("Failed to execute remote query: %s", e.what());
            throw IOException("PostHog: Failed to execute remote query: " + string(e.what()));
        }
    }

    // Check if we have more data to return
    if (!global_state.result_table || global_state.current_row >= static_cast<idx_t>(global_state.result_table->num_rows())) {
        output.SetCardinality(0);
        return;
    }

    // Calculate how many rows to copy
    idx_t remaining = static_cast<idx_t>(global_state.result_table->num_rows()) - global_state.current_row;
    idx_t rows_to_copy = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

    // Convert Arrow data to DuckDB DataChunk
    ArrowConversion::ArrowTableToDataChunk(global_state.result_table, output, global_state.current_row, rows_to_copy);

    global_state.current_row += rows_to_copy;
    output.SetCardinality(rows_to_copy);
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
