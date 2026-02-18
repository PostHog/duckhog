//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_merge.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PostHogCatalog;

class PhysicalPostHogMerge : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalPostHogMerge(PhysicalPlan &physical_plan, vector<LogicalType> types, PostHogCatalog &catalog,
	                     string non_returning_sql, string returning_sql, bool return_chunk,
	                     idx_t estimated_cardinality);

	string GetName() const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

private:
	PostHogCatalog &catalog_;
	string non_returning_sql_;
	string returning_sql_;
	bool return_chunk_;
};

} // namespace duckdb
