//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_insert.hpp
//
// Physical sink/source operator for INSERT into remote PostHog tables
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PostHogCatalog;

class PhysicalPostHogInsert : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalPostHogInsert(PhysicalPlan &physical_plan, vector<LogicalType> types, PostHogCatalog &catalog,
	                      string remote_schema, string remote_table, vector<string> column_names,
	                      idx_t estimated_cardinality);

	string GetName() const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

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
	string QualifyTableName() const;
	string BuildInsertSQL(const DataChunk &chunk) const;

private:
	PostHogCatalog &catalog_;
	string remote_schema_;
	string remote_table_;
	vector<string> column_names_;
};

} // namespace duckdb
