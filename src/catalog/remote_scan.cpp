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

#include <arrow/c/bridge.h>

#include <cstring>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind Data
//===----------------------------------------------------------------------===//

PostHogRemoteScanBindData::PostHogRemoteScanBindData(PostHogCatalog &catalog_p, const string &schema_name_p,
                                                     const string &table_name_p)
    : ArrowScanFunctionData(&PostHogArrowStream::Produce, reinterpret_cast<uintptr_t>(this)),
      catalog(catalog_p), schema_name(schema_name_p), table_name(table_name_p) {
}

PostHogRemoteScanBindData::~PostHogRemoteScanBindData() {
    // Restore the original name pointers so that ArrowSchemaWrapper's destructor
    // (which runs after this one) sees the pointers Arrow's release callback
    // expects.  Then free our strdup'd replacements.
    for (auto &entry : patched_schema_names) {
        entry.child->name = entry.original;
        free(entry.patched);
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
                                                           const vector<LogicalType> &column_types,
                                                           const std::shared_ptr<arrow::Schema> &arrow_schema) {
    auto bind_data = make_uniq<PostHogRemoteScanBindData>(catalog, schema_name, table_name);

    bind_data->column_names = column_names;
    bind_data->column_types = column_types;

    // Export the cached Arrow schema to C ArrowSchema (no Flight RPC).
    auto status = arrow::ExportSchema(*arrow_schema, &bind_data->schema_root.arrow_schema);
    if (!status.ok()) {
        throw IOException("PostHog: Failed to export cached Arrow schema: " + status.ToString());
    }

    // Patch each child's name with the deduplicated catalog column name.
    // DuckDB's PopulateArrowTableSchema deduplicates names (case-insensitive, appending _1, _2, ...),
    // and the planner assigns column IDs from those deduplicated names.  ProduceArrowScan reads
    // children[col_idx]->name to build the projected SQL, so the C ArrowSchema must carry the
    // deduplicated names, not the raw Arrow field names (which may contain duplicates).
    auto &schema_c = bind_data->schema_root.arrow_schema;
    if (static_cast<idx_t>(schema_c.n_children) != column_names.size()) {
        throw InternalException("PostHog: cached Arrow schema has %lld fields but catalog has %llu columns",
                                schema_c.n_children, column_names.size());
    }
    for (idx_t i = 0; i < column_names.size(); i++) {
        auto *child = schema_c.children[i];
        const char *original = child->name;
        char *patched = strdup(column_names[i].c_str());
        child->name = patched;
        PostHogRemoteScanBindData::PatchedName entry;
        entry.child = child;
        entry.original = original;
        entry.patched = patched;
        bind_data->patched_schema_names.push_back(entry);
    }

    // Populate arrow_table (keyed {0, 1, ..., N-1}) and all_types from the full schema.
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
    func.projection_pushdown = true;
    func.filter_pushdown = false;     // TODO: Implement filter pushdown
    func.table_scan_progress = Progress;
    return func;
}

} // namespace duckdb
