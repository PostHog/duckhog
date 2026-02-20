//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/remote_scan.hpp
//
// Table function for executing remote queries via Flight SQL
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "flight/arrow_stream.hpp"

#include <memory>
#include <optional>

namespace arrow {
class Schema;
} // namespace arrow

namespace duckdb {

class PostHogCatalog;

//===----------------------------------------------------------------------===//
// Remote Scan Bind Data
//===----------------------------------------------------------------------===//

struct PostHogRemoteScanBindData : public ArrowScanFunctionData {
	PostHogRemoteScanBindData(PostHogCatalog &catalog, const string &schema_name, const string &table_name);
	~PostHogRemoteScanBindData() override;

	PostHogCatalog &catalog;
	string schema_name;
	string table_name;

	// Column information (populated during bind)
	vector<string> column_names;
	vector<LogicalType> column_types;

	// Optional AT clause SQL fragment, e.g. "AT (VERSION => 1)"
	string at_clause_sql;

	// Patched C ArrowSchema child name pointers.  Each entry records the child
	// schema, the original name pointer (owned by Arrow's private data), and the
	// strdup'd replacement.  The destructor restores originals before the base
	// class releases the ArrowSchema, avoiding a double-free.
	struct PatchedName {
		ArrowSchema *child;
		const char *original;
		char *patched;
	};
	vector<PatchedName> patched_schema_names;
};

// Per-execution stream factory passed to PostHogArrowStream::Produce.
// Owns the transaction id captured during InitGlobal.
struct PostHogRemoteScanStreamFactory {
	const PostHogRemoteScanBindData *bind_data;
	std::optional<TransactionId> txn_id;
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
	                                               const vector<LogicalType> &column_types,
	                                               const std::shared_ptr<arrow::Schema> &arrow_schema);

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
