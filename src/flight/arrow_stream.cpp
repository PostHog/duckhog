//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/arrow_stream.cpp
//
// Arrow C stream bridge for DuckDB's Arrow scan
//===----------------------------------------------------------------------===//

#include "flight/arrow_stream.hpp"

#include "catalog/posthog_catalog.hpp"
#include "duckdb/common/exception.hpp"

#include <arrow/c/bridge.h>

#include <utility>

namespace duckdb {

PostHogArrowStreamState::PostHogArrowStreamState(PostHogCatalog &catalog_p, std::string query_p)
    : catalog(catalog_p), query(std::move(query_p)) {
    query_stream = catalog.GetFlightClient().ExecuteQueryStream(query);
}

void PostHogArrowStream::Initialize(ArrowArrayStream &stream, std::shared_ptr<PostHogArrowStreamState> state) {
    stream.get_schema = StreamGetSchema;
    stream.get_next = StreamGetNext;
    stream.get_last_error = StreamGetLastError;
    stream.release = StreamRelease;
    stream.private_data = new std::shared_ptr<PostHogArrowStreamState>(std::move(state));
}

unique_ptr<ArrowArrayStreamWrapper> PostHogArrowStream::Produce(uintptr_t stream_factory_ptr,
                                                                ArrowStreamParameters &parameters) {
    (void)parameters;
    auto res = make_uniq<ArrowArrayStreamWrapper>();
    res->arrow_array_stream = *reinterpret_cast<ArrowArrayStream *>(stream_factory_ptr);
    return res;
}

void PostHogArrowStream::GetSchema(ArrowArrayStream *stream_factory_ptr, ArrowSchema &schema) {
    if (stream_factory_ptr->get_schema(stream_factory_ptr, &schema)) {
        const char *error = nullptr;
        if (stream_factory_ptr->get_last_error) {
            error = stream_factory_ptr->get_last_error(stream_factory_ptr);
        }
        auto message = error ? string(error) : string("unknown error");
        throw InvalidInputException("PostHog: Arrow stream get_schema failed: %s", message);
    }
}

int PostHogArrowStream::StreamGetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
    if (!stream || !stream->private_data || !out) {
        return -1;
    }
    auto state_holder = static_cast<std::shared_ptr<PostHogArrowStreamState> *>(stream->private_data);
    return ExportSchema(**state_holder, out);
}

int PostHogArrowStream::StreamGetNext(ArrowArrayStream *stream, ArrowArray *out) {
    if (!stream || !stream->private_data || !out) {
        return -1;
    }
    auto state_holder = static_cast<std::shared_ptr<PostHogArrowStreamState> *>(stream->private_data);
    return ExportNext(**state_holder, out);
}

const char *PostHogArrowStream::StreamGetLastError(ArrowArrayStream *stream) {
    if (!stream || !stream->private_data) {
        return "stream was released";
    }
    auto state_holder = static_cast<std::shared_ptr<PostHogArrowStreamState> *>(stream->private_data);
    return (*state_holder)->last_error.c_str();
}

void PostHogArrowStream::StreamRelease(ArrowArrayStream *stream) {
    if (!stream || !stream->release) {
        return;
    }
    stream->release = nullptr;
    stream->get_schema = nullptr;
    stream->get_next = nullptr;
    stream->get_last_error = nullptr;
    auto state_holder = static_cast<std::shared_ptr<PostHogArrowStreamState> *>(stream->private_data);
    delete state_holder;
    stream->private_data = nullptr;
}

int PostHogArrowStream::ExportSchema(PostHogArrowStreamState &state, ArrowSchema *out) {
    auto schema_result = state.query_stream->GetSchema();
    if (!schema_result.ok()) {
        state.last_error = schema_result.status().ToString();
        return -1;
    }
    auto status = arrow::ExportSchema(*schema_result.ValueUnsafe(), out);
    if (!status.ok()) {
        state.last_error = status.ToString();
        return -1;
    }
    return 0;
}

int PostHogArrowStream::ExportNext(PostHogArrowStreamState &state, ArrowArray *out) {
    auto chunk_result = state.query_stream->Next();
    if (!chunk_result.ok()) {
        state.last_error = chunk_result.status().ToString();
        return -1;
    }
    auto chunk = std::move(*chunk_result);
    if (!chunk.data) {
        out->release = nullptr;
        return 0;
    }
    auto status = arrow::ExportRecordBatch(*chunk.data, out);
    if (!status.ok()) {
        state.last_error = status.ToString();
        return -1;
    }
    return 0;
}

} // namespace duckdb
