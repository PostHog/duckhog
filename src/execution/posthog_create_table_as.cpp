//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_create_table_as.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_create_table_as.hpp"

#include "catalog/posthog_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "execution/posthog_sql_utils.hpp"
#include "storage/posthog_transaction.hpp"

namespace duckdb {

namespace {

struct PostHogCTASGlobalSinkState : public GlobalSinkState {
	idx_t insert_count = 0;
};

struct PostHogCTASSourceState : public GlobalSourceState {
	bool finished = false;
};

} // namespace

PhysicalPostHogCreateTableAs::PhysicalPostHogCreateTableAs(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                           PostHogCatalog &catalog,
                                                           unique_ptr<CreateTableInfo> create_info,
                                                           string remote_schema, string remote_table,
                                                           vector<string> column_names, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      catalog_(catalog), create_info_(std::move(create_info)), remote_schema_(std::move(remote_schema)),
      remote_table_(std::move(remote_table)), column_names_(std::move(column_names)) {
}

string PhysicalPostHogCreateTableAs::GetName() const {
	return "POSTHOG_CREATE_TABLE_AS";
}

unique_ptr<GlobalSinkState> PhysicalPostHogCreateTableAs::GetGlobalSinkState(ClientContext &context) const {
	// Send CREATE TABLE DDL to the remote server before any data arrives.
	// create_info_ already has the catalog rewritten to the remote side by PlanCreateTableAs.
	auto ddl = create_info_->ToString();
	auto remote_txn_id = PostHogTransaction::Get(context, catalog_).remote_txn_id;

	try {
		catalog_.GetFlightClient().ExecuteUpdate(ddl, remote_txn_id);
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw IOException("PostHog: CREATE TABLE AS failed during DDL: %s", ex.what());
	}

	return make_uniq<PostHogCTASGlobalSinkState>();
}

SinkResultType PhysicalPostHogCreateTableAs::Sink(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSinkInput &input) const {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto qualified = QualifyRemoteTableName(catalog_.GetRemoteCatalog(), remote_schema_, remote_table_);
	auto sql = BuildInsertSQL(qualified, column_names_, chunk);
	auto remote_txn_id = PostHogTransaction::Get(context.client, catalog_).remote_txn_id;

	int64_t affected = 0;
	try {
		affected = catalog_.GetFlightClient().ExecuteUpdate(sql, remote_txn_id);
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw IOException("PostHog: CREATE TABLE AS failed during INSERT: %s", ex.what());
	}

	auto &sink_state = input.global_state.Cast<PostHogCTASGlobalSinkState>();
	if (affected < 0) {
		sink_state.insert_count += chunk.size();
	} else {
		sink_state.insert_count += NumericCast<idx_t>(affected);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalPostHogCreateTableAs::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                        OperatorSinkFinalizeInput &input) const {
	(void)pipeline;
	(void)event;
	(void)context;
	(void)input;
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalPostHogCreateTableAs::GetGlobalSourceState(ClientContext &context) const {
	(void)context;
	return make_uniq<PostHogCTASSourceState>();
}

SourceResultType PhysicalPostHogCreateTableAs::GetData(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	(void)context;
	auto &source_state = input.global_state.Cast<PostHogCTASSourceState>();
	if (source_state.finished) {
		return SourceResultType::FINISHED;
	}
	source_state.finished = true;

	auto &global_sink = this->sink_state->Cast<PostHogCTASGlobalSinkState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(global_sink.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
