//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/remote_scan.hpp
//
// Table function for executing remote queries via Flight SQL
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "flight/flight_client.hpp"

namespace duckdb {

class PostHogCatalog;

//===----------------------------------------------------------------------===//
// Remote Scan Bind Data
//===----------------------------------------------------------------------===//

struct PostHogRemoteScanBindData : public TableFunctionData {
    PostHogRemoteScanBindData(PostHogCatalog &catalog, const string &schema_name, const string &table_name);

    PostHogCatalog &catalog;
    string schema_name;
    string table_name;

    // Column information (populated during bind)
    vector<string> column_names;
    vector<LogicalType> column_types;

    // The query to execute (generated from table name + column projection)
    string query;
};

//===----------------------------------------------------------------------===//
// Remote Scan State
//===----------------------------------------------------------------------===//

struct PostHogRemoteScanGlobalState : public GlobalTableFunctionState {
    PostHogRemoteScanGlobalState();

    // Result table from remote execution
    std::shared_ptr<arrow::Table> result_table;

    // Current position in the result
    idx_t current_row = 0;

    // Whether we've executed the query
    bool executed = false;

    idx_t MaxThreads() const override {
        return 1; // Single-threaded for now
    }
};

struct PostHogRemoteScanLocalState : public LocalTableFunctionState {
    // Nothing needed for now
};

//===----------------------------------------------------------------------===//
// Remote Scan Function
//===----------------------------------------------------------------------===//

class PostHogRemoteScan {
public:
    // Get the table function definition
    static TableFunction GetFunction();

    // Create bind data for a specific table scan
    static unique_ptr<FunctionData> CreateBindData(PostHogCatalog &catalog, const string &schema_name,
                                                   const string &table_name, const vector<string> &column_names,
                                                   const vector<LogicalType> &column_types);

private:
    // Table function callbacks
    static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names);

    static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input);

    static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state);

    static void Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output);

    static double Progress(ClientContext &context, const FunctionData *bind_data,
                           const GlobalTableFunctionState *global_state);
};

} // namespace duckdb
