//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_insert.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_insert.hpp"

#include "catalog/posthog_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "execution/posthog_sql_utils.hpp"
#include "storage/posthog_transaction.hpp"

namespace duckdb {

namespace {

struct PostHogInsertGlobalState : public GlobalSinkState {
	idx_t insert_count = 0;
};

struct PostHogInsertSourceState : public GlobalSourceState {
	bool finished = false;
};

} // namespace

PhysicalPostHogInsert::PhysicalPostHogInsert(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                             PostHogCatalog &catalog, string remote_schema, string remote_table,
                                             vector<string> column_names, bool on_conflict_do_nothing,
                                             string on_conflict_clause, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      catalog_(catalog), remote_schema_(std::move(remote_schema)), remote_table_(std::move(remote_table)),
      column_names_(std::move(column_names)), on_conflict_do_nothing_(on_conflict_do_nothing),
      on_conflict_clause_(std::move(on_conflict_clause)) {
}

string PhysicalPostHogInsert::GetName() const {
	return "POSTHOG_INSERT";
}

unique_ptr<GlobalSinkState> PhysicalPostHogInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PostHogInsertGlobalState>();
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
	if (source_state.finished) {
		return SourceResultType::FINISHED;
	}
	source_state.finished = true;
	auto &global_sink = this->sink_state->Cast<PostHogInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(global_sink.insert_count)));
	return SourceResultType::FINISHED;
}

string PhysicalPostHogInsert::QualifyTableName() const {
	return QualifyRemoteTableName(catalog_.GetRemoteCatalog(), remote_schema_, remote_table_);
}

string PhysicalPostHogInsert::BuildInsertSQL(const DataChunk &chunk) const {
	return duckdb::BuildInsertSQL(QualifyTableName(), column_names_, chunk, on_conflict_clause_);
}

} // namespace duckdb
