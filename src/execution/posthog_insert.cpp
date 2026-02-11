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
#include "duckdb/parser/keyword_helper.hpp"
#include "storage/posthog_transaction.hpp"

namespace duckdb {

namespace {

struct PostHogInsertGlobalState : public GlobalSinkState {
	idx_t insert_count = 0;
};

struct PostHogInsertSourceState : public GlobalSourceState {
	bool finished = false;
};

string QuoteIdent(const string &ident) {
	return KeywordHelper::WriteOptionallyQuoted(ident);
}

} // namespace

PhysicalPostHogInsert::PhysicalPostHogInsert(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                             PostHogCatalog &catalog, string remote_schema, string remote_table,
                                             vector<string> column_names, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      catalog_(catalog), remote_schema_(std::move(remote_schema)), remote_table_(std::move(remote_table)),
      column_names_(std::move(column_names)) {
}

string PhysicalPostHogInsert::GetName() const {
	return "POSTHOG_INSERT";
}

unique_ptr<GlobalSinkState> PhysicalPostHogInsert::GetGlobalSinkState(ClientContext &context) const {
	(void)context;
	return make_uniq<PostHogInsertGlobalState>();
}

SinkResultType PhysicalPostHogInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto sql = BuildInsertSQL(chunk);
	auto remote_txn_id = PostHogTransaction::Get(context.client, catalog_).remote_txn_id;
	auto affected = catalog_.GetFlightClient().ExecuteUpdate(sql, remote_txn_id);

	auto &sink_state = input.global_state.Cast<PostHogInsertGlobalState>();
	if (affected < 0) {
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

SourceResultType PhysicalPostHogInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
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
	return QuoteIdent(catalog_.GetRemoteCatalog()) + "." + QuoteIdent(remote_schema_) + "." + QuoteIdent(remote_table_);
}

string PhysicalPostHogInsert::BuildInsertSQL(const DataChunk &chunk) const {
	if (chunk.ColumnCount() != column_names_.size()) {
		throw InternalException("PostHog: insert chunk has %llu columns but table has %llu insert columns",
		                        chunk.ColumnCount(), column_names_.size());
	}

	string sql = "INSERT INTO " + QualifyTableName() + " (";
	for (idx_t col_idx = 0; col_idx < column_names_.size(); col_idx++) {
		if (col_idx > 0) {
			sql += ", ";
		}
		sql += QuoteIdent(column_names_[col_idx]);
	}
	sql += ") VALUES ";

	for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		if (row_idx > 0) {
			sql += ", ";
		}
		sql += "(";
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			if (col_idx > 0) {
				sql += ", ";
			}
			sql += chunk.GetValue(col_idx, row_idx).ToSQLString();
		}
		sql += ")";
	}

	sql += ";";
	return sql;
}

} // namespace duckdb
