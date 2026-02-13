//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_update.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_update.hpp"

#include "catalog/posthog_catalog.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "storage/posthog_transaction.hpp"

#include <arrow/scalar.h>

namespace duckdb {

namespace {

struct PostHogUpdateSourceState : public GlobalSourceState {
	PostHogUpdateSourceState(ClientContext &context, const vector<LogicalType> &types, bool return_chunk_p)
	    : return_collection(context, types), return_chunk(return_chunk_p) {
	}

	bool initialized = false;
	bool return_chunk;
	int64_t affected_rows = 0;
	ColumnDataCollection return_collection;
	ColumnDataScanState scan_state;
};

Value ArrowScalarToValue(const std::shared_ptr<arrow::Scalar> &scalar, const LogicalType &type) {
	if (!scalar || !scalar->is_valid) {
		return Value(type);
	}

	switch (type.id()) {
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(NumericCast<int32_t>(static_cast<arrow::Int32Scalar &>(*scalar).value));
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(NumericCast<int64_t>(static_cast<arrow::Int64Scalar &>(*scalar).value));
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericCast<int16_t>(static_cast<arrow::Int16Scalar &>(*scalar).value));
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericCast<int8_t>(static_cast<arrow::Int8Scalar &>(*scalar).value));
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericCast<uint8_t>(static_cast<arrow::UInt8Scalar &>(*scalar).value));
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericCast<uint16_t>(static_cast<arrow::UInt16Scalar &>(*scalar).value));
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericCast<uint32_t>(static_cast<arrow::UInt32Scalar &>(*scalar).value));
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(NumericCast<uint64_t>(static_cast<arrow::UInt64Scalar &>(*scalar).value));
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(static_cast<arrow::DoubleScalar &>(*scalar).value);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(static_cast<arrow::FloatScalar &>(*scalar).value);
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(static_cast<arrow::BooleanScalar &>(*scalar).value);
	case LogicalTypeId::VARCHAR:
		if (scalar->type->id() == arrow::Type::STRING) {
			return Value(static_cast<arrow::StringScalar &>(*scalar).value->ToString());
		}
		if (scalar->type->id() == arrow::Type::LARGE_STRING) {
			return Value(static_cast<arrow::LargeStringScalar &>(*scalar).value->ToString());
		}
		break;
	default:
		break;
	}

	throw NotImplementedException("PostHog: unsupported RETURNING value type conversion for %s", type.ToString());
}

} // namespace

PhysicalPostHogUpdate::PhysicalPostHogUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                             PostHogCatalog &catalog, string non_returning_sql, string returning_sql,
                                             bool return_chunk, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      catalog_(catalog), non_returning_sql_(std::move(non_returning_sql)), returning_sql_(std::move(returning_sql)),
      return_chunk_(return_chunk) {
}

string PhysicalPostHogUpdate::GetName() const {
	return "POSTHOG_UPDATE";
}

unique_ptr<GlobalSourceState> PhysicalPostHogUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PostHogUpdateSourceState>(context, GetTypes(), return_chunk_);
}

SourceResultType PhysicalPostHogUpdate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<PostHogUpdateSourceState>();
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
				throw IOException("PostHog: failed to combine UPDATE RETURNING batches: %s",
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
					auto scalar_result = combined->column(NumericCast<int>(col_idx))->GetScalar(row_idx);
					if (!scalar_result.ok()) {
						throw IOException("PostHog: failed to read UPDATE RETURNING scalar: %s",
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
