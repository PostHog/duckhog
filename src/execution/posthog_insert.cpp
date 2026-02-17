//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_insert.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_insert.hpp"

#include "catalog/posthog_catalog.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "execution/posthog_sql_utils.hpp"
#include "storage/posthog_transaction.hpp"

namespace duckdb {

namespace {

struct PostHogInsertGlobalState : public GlobalSinkState {
	PostHogInsertGlobalState(ClientContext &context, const vector<LogicalType> &types, bool return_chunk_p)
	    : return_collection(context, types), return_chunk(return_chunk_p) {
	}

	idx_t insert_count = 0;
	ColumnDataCollection return_collection;
	bool return_chunk;
};

struct PostHogInsertSourceState : public GlobalSourceState {
	ColumnDataScanState scan_state;
	bool initialized = false;
	bool finished = false;
};

} // namespace

PhysicalPostHogInsert::PhysicalPostHogInsert(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                             PostHogCatalog &catalog, string remote_schema, string remote_table,
                                             vector<string> column_names, bool return_chunk,
                                             bool on_conflict_do_nothing, string on_conflict_clause,
                                             vector<idx_t> return_input_index_map, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      catalog_(catalog), remote_schema_(std::move(remote_schema)), remote_table_(std::move(remote_table)),
      column_names_(std::move(column_names)), return_chunk_(return_chunk),
      on_conflict_do_nothing_(on_conflict_do_nothing), on_conflict_clause_(std::move(on_conflict_clause)),
      return_input_index_map_(std::move(return_input_index_map)) {
}

string PhysicalPostHogInsert::GetName() const {
	return "POSTHOG_INSERT";
}

unique_ptr<GlobalSinkState> PhysicalPostHogInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PostHogInsertGlobalState>(context, GetTypes(), return_chunk_);
}

SinkResultType PhysicalPostHogInsert::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto sql = BuildInsertSQL(chunk);
	auto remote_txn_id = PostHogTransaction::Get(context.client, catalog_).remote_txn_id;
	int64_t affected = 0;
	try {
		affected = catalog_.GetFlightClient().ExecuteUpdate(sql, remote_txn_id);
	} catch (const Exception &) {
		// ensure duckdb::Exceptions are bubbled up instead of being caught in next catch
		throw;
	} catch (const std::exception &ex) {
		throw IOException("PostHog: INSERT into %s failed for chunk with %llu row(s): %s", QualifyTableName(),
		                  chunk.size(), ex.what());
	}

	auto &sink_state = input.global_state.Cast<PostHogInsertGlobalState>();
	if (affected < 0) {
		if (on_conflict_do_nothing_) {
			throw NotImplementedException(
			    "PostHog: INSERT ... ON CONFLICT DO NOTHING requires an affected-row count from remote server");
		}
		sink_state.insert_count += chunk.size();
	} else {
		sink_state.insert_count += NumericCast<idx_t>(affected);
	}
	if (sink_state.return_chunk) {
		if (return_input_index_map_.empty()) {
			sink_state.return_collection.Append(chunk);
		} else {
			bool needs_projection = return_input_index_map_.size() != chunk.ColumnCount();
			if (!needs_projection) {
				for (idx_t i = 0; i < return_input_index_map_.size(); i++) {
					if (return_input_index_map_[i] != i) {
						needs_projection = true;
						break;
					}
				}
			}
			if (needs_projection) {
				DataChunk projected_chunk;
				projected_chunk.Initialize(Allocator::Get(context.client), GetTypes());
				projected_chunk.SetCardinality(chunk);
				for (idx_t col_idx = 0; col_idx < return_input_index_map_.size(); col_idx++) {
					auto source_idx = return_input_index_map_[col_idx];
					if (source_idx >= chunk.ColumnCount()) {
						throw InternalException("PostHog: return column map index %llu exceeds insert chunk width %llu",
						                        source_idx, chunk.ColumnCount());
					}
					projected_chunk.data[col_idx].Reference(chunk.data[source_idx]);
				}
				sink_state.return_collection.Append(projected_chunk);
			} else {
				sink_state.return_collection.Append(chunk);
			}
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalPostHogInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	(void)pipeline;
	(void)event;
	(void)context;
	(void)input;
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalPostHogInsert::GetGlobalSourceState(ClientContext &context) const {
	(void)context;
	return make_uniq<PostHogInsertSourceState>();
}

SourceResultType PhysicalPostHogInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	(void)context;
	auto &source_state = input.global_state.Cast<PostHogInsertSourceState>();
	auto &global_sink = this->sink_state->Cast<PostHogInsertGlobalState>();
	if (!global_sink.return_chunk) {
		if (source_state.finished) {
			return SourceResultType::FINISHED;
		}
		source_state.finished = true;
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(global_sink.insert_count)));
		return SourceResultType::FINISHED;
	}

	if (!source_state.initialized) {
		global_sink.return_collection.InitializeScan(source_state.scan_state);
		source_state.initialized = true;
	}
	global_sink.return_collection.Scan(source_state.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalPostHogInsert::QualifyTableName() const {
	return QualifyRemoteTableName(catalog_.GetRemoteCatalog(), remote_schema_, remote_table_);
}

string PhysicalPostHogInsert::BuildInsertSQL(const DataChunk &chunk) const {
	return duckdb::BuildInsertSQL(QualifyTableName(), column_names_, chunk, on_conflict_clause_);
}

} // namespace duckdb
