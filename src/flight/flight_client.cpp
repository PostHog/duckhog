//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/flight_client.cpp
//
// Arrow Flight SQL client wrapper implementation
//===----------------------------------------------------------------------===//

#include "flight/flight_client.hpp"

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <stdexcept>

namespace duckdb {

PostHogFlightClient::PostHogFlightClient(const std::string &endpoint, const std::string &token)
    : endpoint_(endpoint), token_(token) {

    // Parse endpoint and create location
    auto location_result = arrow::flight::Location::Parse(endpoint);
    if (!location_result.ok()) {
        throw std::runtime_error("PostHog: Invalid Flight endpoint '" + endpoint + "': " +
                                 location_result.status().ToString());
    }
    auto location = *location_result;

    // Create Flight client options
    arrow::flight::FlightClientOptions options;

    // For TLS endpoints, we might need to configure certificates
    // For now, use default options which work for non-TLS connections
    // TODO: Add TLS configuration support

    // Connect to the Flight server
    auto client_result = arrow::flight::FlightClient::Connect(location, options);
    if (!client_result.ok()) {
        throw std::runtime_error("PostHog: Failed to connect to Flight server at '" + endpoint + "': " +
                                 client_result.status().ToString());
    }

    // FlightSqlClient takes ownership of the FlightClient via shared_ptr
    std::shared_ptr<arrow::flight::FlightClient> flight_client = std::move(*client_result);

    // Wrap in SQL client for Flight SQL protocol support
    sql_client_ = std::make_unique<arrow::flight::sql::FlightSqlClient>(std::move(flight_client));
}

PostHogFlightClient::~PostHogFlightClient() = default;

void PostHogFlightClient::Authenticate() {
    // For bearer token authentication, we simply mark as authenticated
    // The actual token is passed in the headers with each request via GetCallOptions()
    //
    // For more complex authentication flows (username/password, OAuth, etc.),
    // this method would call the Authenticate RPC
    authenticated_ = true;
}

arrow::flight::FlightCallOptions PostHogFlightClient::GetCallOptions() const {
    arrow::flight::FlightCallOptions options;

    // Add bearer token as authorization header
    if (!token_.empty()) {
        options.headers.push_back({"authorization", "Bearer " + token_});
    }

    return options;
}

std::shared_ptr<arrow::Table> PostHogFlightClient::ExecuteQuery(const std::string &sql) {
    std::lock_guard<std::mutex> lock(client_mutex_);

    if (!authenticated_) {
        throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
    }

    auto call_options = GetCallOptions();

    // Execute the query via Flight SQL Execute RPC
    auto info_result = sql_client_->Execute(call_options, sql);
    if (!info_result.ok()) {
        throw std::runtime_error("PostHog: Query execution failed: " + info_result.status().ToString());
    }

    auto flight_info = std::move(*info_result);

    // Collect all result batches from all endpoints
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::shared_ptr<arrow::Schema> result_schema;

    for (const auto &endpoint : flight_info->endpoints()) {
        // Get the result stream for this endpoint
        auto stream_result = sql_client_->DoGet(call_options, endpoint.ticket);
        if (!stream_result.ok()) {
            throw std::runtime_error("PostHog: Failed to fetch results: " + stream_result.status().ToString());
        }

        auto stream = std::move(*stream_result);

        // Read all batches from the stream
        while (true) {
            auto chunk_result = stream->Next();
            if (!chunk_result.ok()) {
                throw std::runtime_error("PostHog: Failed to read result batch: " + chunk_result.status().ToString());
            }

            auto chunk = std::move(*chunk_result);
            if (!chunk.data) {
                break; // End of stream
            }

            if (!result_schema) {
                result_schema = chunk.data->schema();
            }
            batches.push_back(chunk.data);
        }
    }

    // Combine all batches into a single table
    if (batches.empty()) {
        // Return empty table - need schema from FlightInfo
        auto schema_result = flight_info->GetSchema(nullptr);
        if (schema_result.ok()) {
            auto empty_result = arrow::Table::MakeEmpty(*schema_result);
            if (empty_result.ok()) {
                return *empty_result;
            }
        }
        // Fallback: return empty table with no columns
        std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
        return arrow::Table::Make(arrow::schema({}), empty_columns);
    }

    auto table_result = arrow::Table::FromRecordBatches(batches);
    if (!table_result.ok()) {
        throw std::runtime_error("PostHog: Failed to combine result batches: " + table_result.status().ToString());
    }

    return *table_result;
}

std::unique_ptr<PostHogFlightQueryStream> PostHogFlightClient::ExecuteQueryStream(const std::string &sql) {
    std::lock_guard<std::mutex> lock(client_mutex_);

    if (!authenticated_) {
        throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
    }

    auto call_options = GetCallOptions();

    auto info_result = sql_client_->Execute(call_options, sql);
    if (!info_result.ok()) {
        throw std::runtime_error("PostHog: Query execution failed: " + info_result.status().ToString());
    }

    return std::make_unique<PostHogFlightQueryStream>(*sql_client_, call_options, std::move(*info_result));
}

std::shared_ptr<arrow::Schema> PostHogFlightClient::GetQuerySchema(const std::string &sql) {
    std::lock_guard<std::mutex> lock(client_mutex_);

    if (!authenticated_) {
        throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
    }

    auto call_options = GetCallOptions();

    // Use Prepare to get schema without full execution
    auto prepared_result = sql_client_->Prepare(call_options, sql);
    if (!prepared_result.ok()) {
        throw std::runtime_error("PostHog: Failed to prepare query: " + prepared_result.status().ToString());
    }

    auto prepared_statement = std::move(*prepared_result);

    // Get the result schema from the dataset schema
    return prepared_statement->dataset_schema();
}

std::vector<std::string> PostHogFlightClient::ListSchemas() {
    std::lock_guard<std::mutex> lock(client_mutex_);

    if (!authenticated_) {
        throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
    }

    auto call_options = GetCallOptions();

    // GetDbSchemas returns information about schemas/catalogs
    // Parameters: options, catalog (nullptr = all), db_schema_filter_pattern (nullptr = all)
    auto info_result = sql_client_->GetDbSchemas(call_options, nullptr, nullptr);
    if (!info_result.ok()) {
        throw std::runtime_error("PostHog: Failed to list schemas: " + info_result.status().ToString());
    }

    std::vector<std::string> schemas;
    auto flight_info = std::move(*info_result);

    if (flight_info->endpoints().empty()) {
        return schemas;
    }

    // Fetch the schema list
    auto stream_result = sql_client_->DoGet(call_options, flight_info->endpoints()[0].ticket);
    if (!stream_result.ok()) {
        throw std::runtime_error("PostHog: Failed to fetch schema list: " + stream_result.status().ToString());
    }

    auto stream = std::move(*stream_result);

    while (true) {
        auto chunk_result = stream->Next();
        if (!chunk_result.ok()) {
            throw std::runtime_error("PostHog: Failed to read schema list: " + chunk_result.status().ToString());
        }

        auto chunk = std::move(*chunk_result);
        if (!chunk.data) {
            break;
        }

        // RecordBatch::GetColumnByName returns Array, not ChunkedArray
        auto schema_col = chunk.data->GetColumnByName("db_schema_name");
        if (!schema_col) {
            schema_col = chunk.data->GetColumnByName("schema_name");
        }

        if (schema_col) {
            auto schema_array = std::static_pointer_cast<arrow::StringArray>(schema_col);
            for (int64_t i = 0; i < schema_array->length(); i++) {
                if (!schema_array->IsNull(i)) {
                    schemas.push_back(std::string(schema_array->GetView(i)));
                }
            }
        }
    }

    return schemas;
}

std::vector<std::string> PostHogFlightClient::ListTables(const std::string &schema) {
    std::lock_guard<std::mutex> lock(client_mutex_);

    if (!authenticated_) {
        throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
    }

    auto call_options = GetCallOptions();

    // GetTables returns information about tables
    // Parameters: catalog, schema_filter_pattern, table_name_filter_pattern, include_schema, table_types
    auto info_result = sql_client_->GetTables(call_options, nullptr, &schema, nullptr, false, nullptr);
    if (!info_result.ok()) {
        throw std::runtime_error("PostHog: Failed to list tables: " + info_result.status().ToString());
    }

    std::vector<std::string> tables;
    auto flight_info = std::move(*info_result);

    if (flight_info->endpoints().empty()) {
        return tables;
    }

    // Fetch the table list
    auto stream_result = sql_client_->DoGet(call_options, flight_info->endpoints()[0].ticket);
    if (!stream_result.ok()) {
        throw std::runtime_error("PostHog: Failed to fetch table list: " + stream_result.status().ToString());
    }

    auto stream = std::move(*stream_result);

    while (true) {
        auto chunk_result = stream->Next();
        if (!chunk_result.ok()) {
            throw std::runtime_error("PostHog: Failed to read table list: " + chunk_result.status().ToString());
        }

        auto chunk = std::move(*chunk_result);
        if (!chunk.data) {
            break;
        }

        // RecordBatch::GetColumnByName returns Array, not ChunkedArray
        auto table_col = chunk.data->GetColumnByName("table_name");
        if (table_col) {
            auto table_array = std::static_pointer_cast<arrow::StringArray>(table_col);
            for (int64_t i = 0; i < table_array->length(); i++) {
                if (!table_array->IsNull(i)) {
                    tables.push_back(std::string(table_array->GetView(i)));
                }
            }
        }
    }

    return tables;
}

std::shared_ptr<arrow::Schema> PostHogFlightClient::GetTableSchema(const std::string &schema,
                                                                    const std::string &table) {
    std::lock_guard<std::mutex> lock(client_mutex_);

    if (!authenticated_) {
        throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
    }

    auto call_options = GetCallOptions();

    // GetTables with include_schema=true returns serialized schema in the result
    auto info_result = sql_client_->GetTables(call_options, nullptr, &schema, &table, true, nullptr);
    if (!info_result.ok()) {
        throw std::runtime_error("PostHog: Failed to get table schema: " + info_result.status().ToString());
    }

    auto flight_info = std::move(*info_result);

    if (flight_info->endpoints().empty()) {
        throw std::runtime_error("PostHog: Table not found: " + schema + "." + table);
    }

    // Fetch the table metadata
    auto stream_result = sql_client_->DoGet(call_options, flight_info->endpoints()[0].ticket);
    if (!stream_result.ok()) {
        throw std::runtime_error("PostHog: Failed to fetch table metadata: " + stream_result.status().ToString());
    }

    auto stream = std::move(*stream_result);
    auto chunk_result = stream->Next();
    if (!chunk_result.ok()) {
        throw std::runtime_error("PostHog: Failed to read table metadata: " + chunk_result.status().ToString());
    }

    auto chunk = std::move(*chunk_result);
    if (!chunk.data || chunk.data->num_rows() == 0) {
        throw std::runtime_error("PostHog: Table not found: " + schema + "." + table);
    }

    // The table_schema column contains the IPC-serialized Arrow schema
    // RecordBatch::GetColumnByName returns Array, not ChunkedArray
    auto schema_col = chunk.data->GetColumnByName("table_schema");
    if (!schema_col) {
        throw std::runtime_error("PostHog: Server did not return table schema for: " + schema + "." + table);
    }

    auto table_name_col = chunk.data->GetColumnByName("table_name");
    if (!table_name_col) {
        throw std::runtime_error("PostHog: Server did not return table_name column");
    }

    auto schema_array = std::static_pointer_cast<arrow::BinaryArray>(schema_col);
    auto table_name_array = std::static_pointer_cast<arrow::StringArray>(table_name_col);

    // Find the row matching the requested table name
    int64_t row_idx = -1;
    for (int64_t i = 0; i < chunk.data->num_rows(); i++) {
        if (!table_name_array->IsNull(i) && table_name_array->GetView(i) == table) {
            row_idx = i;
            break;
        }
    }

    if (row_idx < 0) {
        throw std::runtime_error("PostHog: Table not found in metadata: " + schema + "." + table);
    }

    if (schema_array->IsNull(row_idx)) {
        throw std::runtime_error("PostHog: Table schema is null for: " + schema + "." + table);
    }

    auto schema_bytes = schema_array->GetView(row_idx);

    // Deserialize the Arrow schema from IPC format
    arrow::ipc::DictionaryMemo dict_memo;
    auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(schema_bytes.data()),
                                                  static_cast<int64_t>(schema_bytes.size()));
    arrow::io::BufferReader reader(buffer);

    auto schema_read_result = arrow::ipc::ReadSchema(&reader, &dict_memo);
    if (!schema_read_result.ok()) {
        throw std::runtime_error("PostHog: Failed to deserialize table schema: " +
                                 schema_read_result.status().ToString());
    }

    return *schema_read_result;
}

PostHogFlightQueryStream::PostHogFlightQueryStream(arrow::flight::sql::FlightSqlClient &client,
                                                   arrow::flight::FlightCallOptions options,
                                                   std::unique_ptr<arrow::flight::FlightInfo> info)
    : client_(client), options_(std::move(options)), info_(std::move(info)) {
}

arrow::Status PostHogFlightQueryStream::OpenReader() {
    if (reader_) {
        return arrow::Status::OK();
    }
    if (!info_ || info_->endpoints().empty()) {
        return arrow::Status::Invalid("FlightInfo did not return any endpoints");
    }
    if (endpoint_index_ >= info_->endpoints().size()) {
        return arrow::Status::OK();
    }
    auto stream_result = client_.DoGet(options_, info_->endpoints()[endpoint_index_].ticket);
    if (!stream_result.ok()) {
        return stream_result.status();
    }
    reader_ = std::move(*stream_result);
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Schema>> PostHogFlightQueryStream::GetSchema() {
    if (schema_) {
        return schema_;
    }
    if (info_) {
        auto schema_result = info_->GetSchema(nullptr);
        if (schema_result.ok()) {
            schema_ = *schema_result;
            return schema_;
        }
    }
    auto status = OpenReader();
    if (!status.ok()) {
        return status;
    }
    auto schema_result = reader_->GetSchema();
    if (!schema_result.ok()) {
        return schema_result.status();
    }
    schema_ = *schema_result;
    return schema_;
}

arrow::Result<arrow::flight::FlightStreamChunk> PostHogFlightQueryStream::Next() {
    while (true) {
        auto status = OpenReader();
        if (!status.ok()) {
            return status;
        }
        if (!reader_) {
            return arrow::flight::FlightStreamChunk();
        }
        auto chunk_result = reader_->Next();
        if (!chunk_result.ok()) {
            return chunk_result.status();
        }
        auto chunk = std::move(*chunk_result);
        if (chunk.data) {
            return chunk;
        }
        reader_.reset();
        endpoint_index_++;
        if (!info_ || endpoint_index_ >= info_->endpoints().size()) {
            return chunk;
        }
    }
}

} // namespace duckdb
