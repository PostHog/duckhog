//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/arrow_stream.hpp
//
// Arrow C stream bridge for DuckDB's Arrow scan
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "flight/flight_client.hpp"

#include <memory>
#include <optional>
#include <string>

namespace duckdb {

class PostHogCatalog;

struct PostHogArrowStreamState {
	PostHogArrowStreamState(PostHogCatalog &catalog, std::string query, std::optional<TransactionId> txn_id);

	PostHogCatalog &catalog;
	std::string query;
	std::optional<TransactionId> txn_id;
	std::unique_ptr<PostHogFlightQueryStream> query_stream;
	std::string last_error;
	bool released = false;
};

class PostHogArrowStream {
public:
	static void Initialize(ArrowArrayStream &stream, std::shared_ptr<PostHogArrowStreamState> state);
	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t stream_factory_ptr, ArrowStreamParameters &parameters);
	static void GetSchema(ArrowArrayStream *stream_factory_ptr, ArrowSchema &schema);

private:
	static int StreamGetSchema(ArrowArrayStream *stream, ArrowSchema *out);
	static int StreamGetNext(ArrowArrayStream *stream, ArrowArray *out);
	static const char *StreamGetLastError(ArrowArrayStream *stream);
	static void StreamRelease(ArrowArrayStream *stream);

	static int ExportSchema(PostHogArrowStreamState &state, ArrowSchema *out);
	static int ExportNext(PostHogArrowStreamState &state, ArrowArray *out);
};

} // namespace duckdb
