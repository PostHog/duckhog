//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_create_table_as.hpp
//
// Physical sink/source operator for CREATE TABLE AS on remote PostHog tables.
// Sends CREATE TABLE DDL in GetGlobalSinkState, streams INSERT data in Sink,
// returns row count in GetData.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

class PostHogCatalog;

class PhysicalPostHogCreateTableAs : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalPostHogCreateTableAs(PhysicalPlan &physical_plan, vector<LogicalType> types, PostHogCatalog &catalog,
	                             unique_ptr<CreateTableInfo> create_info, string remote_schema, string remote_table,
	                             vector<string> column_names, idx_t estimated_cardinality);

	string GetName() const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	bool SinkOrderDependent() const override {
		return true;
	}

	bool IsSource() const override {
		return true;
	}

private:
	PostHogCatalog &catalog_;
	unique_ptr<CreateTableInfo> create_info_;
	string remote_schema_;
	string remote_table_;
	vector<string> column_names_;
};

} // namespace duckdb
