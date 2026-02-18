//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_merge.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_merge.hpp"

#include "catalog/posthog_catalog.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "storage/posthog_transaction.hpp"
#include "utils/arrow_value.hpp"

namespace duckdb {

namespace {

struct PostHogMergeSourceState : public GlobalSourceState {
	PostHogMergeSourceState(ClientContext &context, const vector<LogicalType> &types, bool return_chunk_p)
	    : return_collection(context, types), return_chunk(return_chunk_p) {
	}

	bool initialized = false;
	bool return_chunk;
	int64_t affected_rows = 0;
	ColumnDataCollection return_collection;
	ColumnDataScanState scan_state;
};

} // namespace

PhysicalPostHogMerge::PhysicalPostHogMerge(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                           PostHogCatalog &catalog, string non_returning_sql, string returning_sql,
                                           bool return_chunk, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      catalog_(catalog), non_returning_sql_(std::move(non_returning_sql)), returning_sql_(std::move(returning_sql)),
      return_chunk_(return_chunk) {
}

string PhysicalPostHogMerge::GetName() const {
	return "POSTHOG_MERGE";
}

unique_ptr<GlobalSourceState> PhysicalPostHogMerge::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PostHogMergeSourceState>(context, GetTypes(), return_chunk_);
}

SourceResultType PhysicalPostHogMerge::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<PostHogMergeSourceState>();
	if (!state.initialized) {
		auto remote_txn_id = PostHogTransaction::Get(context.client, catalog_).remote_txn_id;
		if (!state.return_chunk) {
			state.affected_rows = catalog_.GetFlightClient().ExecuteUpdate(non_returning_sql_, remote_txn_id);
		} else {
			auto result = catalog_.GetFlightClient().ExecuteQuery(returning_sql_, remote_txn_id);
			DataChunk output_chunk;
			output_chunk.Initialize(Allocator::Get(context.client), GetTypes());

			auto combined_result = result->CombineChunksToBatch();
			if (!combined_result.ok()) {
				throw IOException("PostHog: failed to combine MERGE RETURNING batches: %s",
				                  combined_result.status().ToString());
			}
			const auto &combined = *combined_result;
			for (int64_t row_idx = 0; row_idx < combined->num_rows(); row_idx++) {
				if ((idx_t)output_chunk.size() == STANDARD_VECTOR_SIZE) {
					state.return_collection.Append(output_chunk);
					output_chunk.Reset();
				}
				auto out_row = output_chunk.size();
				output_chunk.SetCardinality(out_row + 1);
				for (idx_t col_idx = 0; col_idx < GetTypes().size(); col_idx++) {
					auto scalar_result = combined->column(static_cast<int>(col_idx))->GetScalar(row_idx);
					if (!scalar_result.ok()) {
						throw IOException("PostHog: failed to read MERGE RETURNING scalar: %s",
						                  scalar_result.status().ToString());
					}
					output_chunk.SetValue(col_idx, out_row, ArrowScalarToValue(*scalar_result, GetTypes()[col_idx]));
				}
			}
			if (output_chunk.size() > 0) {
				state.return_collection.Append(output_chunk);
			}
			state.return_collection.InitializeScan(state.scan_state);
		}
		state.initialized = true;
	}

	if (!state.return_chunk) {
		chunk.Reset();
		if (state.affected_rows == NumericLimits<int64_t>::Minimum()) {
			return SourceResultType::FINISHED;
		}
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(state.affected_rows));
		state.affected_rows = NumericLimits<int64_t>::Minimum();
		return SourceResultType::FINISHED;
	}

	state.return_collection.Scan(state.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
