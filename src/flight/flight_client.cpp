//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/flight_client.cpp
//
// Arrow Flight SQL client wrapper implementation
//===----------------------------------------------------------------------===//

#include "flight/flight_client.hpp"

#include <arrow/api.h>
#include <arrow/device.h>
#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/options.h>
#include <arrow/ipc/reader.h>
#include <iostream>
#include <string_view>
#include <stdexcept>

namespace duckdb {

namespace {
std::string Base64Encode(const std::string &input) {
	static const char kBase64Alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	std::string output;
	output.reserve(((input.size() + 2) / 3) * 4);

	size_t i = 0;
	while (i + 3 <= input.size()) {
		const unsigned char b0 = static_cast<unsigned char>(input[i++]);
		const unsigned char b1 = static_cast<unsigned char>(input[i++]);
		const unsigned char b2 = static_cast<unsigned char>(input[i++]);
		output.push_back(kBase64Alphabet[(b0 >> 2) & 0x3F]);
		output.push_back(kBase64Alphabet[((b0 & 0x03) << 4) | ((b1 >> 4) & 0x0F)]);
		output.push_back(kBase64Alphabet[((b1 & 0x0F) << 2) | ((b2 >> 6) & 0x03)]);
		output.push_back(kBase64Alphabet[b2 & 0x3F]);
	}

	const size_t rem = input.size() - i;
	if (rem == 1) {
		const unsigned char b0 = static_cast<unsigned char>(input[i]);
		output.push_back(kBase64Alphabet[(b0 >> 2) & 0x3F]);
		output.push_back(kBase64Alphabet[(b0 & 0x03) << 4]);
		output.push_back('=');
		output.push_back('=');
	} else if (rem == 2) {
		const unsigned char b0 = static_cast<unsigned char>(input[i++]);
		const unsigned char b1 = static_cast<unsigned char>(input[i]);
		output.push_back(kBase64Alphabet[(b0 >> 2) & 0x3F]);
		output.push_back(kBase64Alphabet[((b0 & 0x03) << 4) | ((b1 >> 4) & 0x0F)]);
		output.push_back(kBase64Alphabet[(b1 & 0x0F) << 2]);
		output.push_back('=');
	}

	return output;
}
} // namespace

PostHogFlightClient::PostHogFlightClient(const std::string &endpoint, const std::string &user,
                                         const std::string &password, bool tls_skip_verify)
    : endpoint_(endpoint), user_(user), password_(password) {
	// Parse endpoint and create location
	auto location_result = arrow::flight::Location::Parse(endpoint);
	if (!location_result.ok()) {
		throw std::runtime_error("PostHog: Invalid Flight endpoint '" + endpoint +
		                         "': " + location_result.status().ToString());
	}
	const auto &location = *location_result;

	// Create Flight client options
	arrow::flight::FlightClientOptions options;

	// Secure by default: verify server certificates unless explicitly overridden.
	options.disable_server_verification = tls_skip_verify;

	// Connect to the Flight server
	auto client_result = arrow::flight::FlightClient::Connect(location, options);
	if (!client_result.ok()) {
		throw std::runtime_error("PostHog: Failed to connect to Flight server at '" + endpoint +
		                         "': " + client_result.status().ToString());
	}

	// FlightSqlClient takes ownership of the FlightClient via shared_ptr
	std::shared_ptr<arrow::flight::FlightClient> flight_client = std::move(*client_result);

	// Wrap in SQL client for Flight SQL protocol support
	sql_client_ = std::make_unique<arrow::flight::sql::FlightSqlClient>(std::move(flight_client));
}

PostHogFlightClient::~PostHogFlightClient() = default;

void PostHogFlightClient::Authenticate() {
	if (user_.empty() || password_.empty()) {
		throw std::runtime_error("PostHog: Missing Flight credentials (user/password)");
	}
	authenticated_ = true;
}

arrow::Status PostHogFlightClient::Ping() {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		return arrow::Status::Invalid("Not authenticated. Call Authenticate() first.");
	}
	if (!sql_client_) {
		return arrow::Status::Invalid("SQL client not initialized");
	}

	auto call_options = GetCallOptions();
	// Prefer a metadata RPC that our servers/tests already implement (GetDbSchemas),
	// since some Flight SQL servers may not implement SqlInfo.
	auto info_result = sql_client_->GetDbSchemas(call_options, nullptr, nullptr);
	if (!info_result.ok()) {
		return info_result.status();
	}

	auto info = std::move(*info_result);
	if (!info || info->endpoints().empty()) {
		return arrow::Status::OK();
	}

	// Drain the stream so server-side readers are fully released on single-conn
	// sessions where one open result stream can block subsequent statements.
	auto stream_result = sql_client_->DoGet(call_options, info->endpoints()[0].ticket);
	if (!stream_result.ok()) {
		return stream_result.status();
	}
	auto stream = std::move(*stream_result);
	while (true) {
		auto chunk_result = stream->Next();
		if (!chunk_result.ok()) {
			return chunk_result.status();
		}
		const auto &chunk = *chunk_result;
		if (!chunk.data) {
			break;
		}
	}

	return arrow::Status::OK();
}

arrow::flight::FlightCallOptions PostHogFlightClient::GetCallOptions() const {
	arrow::flight::FlightCallOptions options;

	// Add HTTP Basic credentials (username/password) for each request.
	if (!user_.empty() && !password_.empty()) {
		options.headers.emplace_back("authorization", "Basic " + Base64Encode(user_ + ":" + password_));
	}

	// Control new allocations Arrow performs while decoding.
	options.memory_manager = arrow::default_cpu_memory_manager();
	options.read_options = arrow::ipc::IpcReadOptions::Defaults();

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

			const auto &chunk = *chunk_result;
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
		arrow::ipc::DictionaryMemo memo;
		auto schema_result = flight_info->GetSchema(&memo);
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

int64_t PostHogFlightClient::ExecuteUpdate(const std::string &sql, const std::optional<TransactionId> &txn_id) {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();

	arrow::Result<int64_t> result;
	if (txn_id.has_value()) {
		arrow::flight::sql::Transaction txn(*txn_id);
		result = sql_client_->ExecuteUpdate(call_options, sql, txn);
	} else {
		result = sql_client_->ExecuteUpdate(call_options, sql);
	}
	if (!result.ok()) {
		throw std::runtime_error("PostHog: Update execution failed: " + result.status().ToString());
	}

	return *result;
}

std::unique_ptr<PostHogFlightQueryStream>
PostHogFlightClient::ExecuteQueryStream(const std::string &sql, const std::optional<TransactionId> &txn_id) {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> info_result;
	if (txn_id.has_value()) {
		arrow::flight::sql::Transaction txn(*txn_id);
		info_result = sql_client_->Execute(call_options, sql, txn);
	} else {
		info_result = sql_client_->Execute(call_options, sql);
	}
	if (!info_result.ok()) {
		throw std::runtime_error("PostHog: Query execution failed: " + info_result.status().ToString());
	}

	return std::make_unique<PostHogFlightQueryStream>(*sql_client_, client_mutex_, call_options,
	                                                  std::move(*info_result));
}

std::shared_ptr<arrow::Schema> PostHogFlightClient::GetQuerySchema(const std::string &sql,
                                                                   const std::optional<TransactionId> &txn_id) {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();

	// Use Prepare to get schema without full execution
	arrow::Result<std::shared_ptr<arrow::flight::sql::PreparedStatement>> prepared_result;
	if (txn_id.has_value()) {
		arrow::flight::sql::Transaction txn(*txn_id);
		prepared_result = sql_client_->Prepare(call_options, sql, txn);
	} else {
		prepared_result = sql_client_->Prepare(call_options, sql);
	}
	if (!prepared_result.ok()) {
		throw std::runtime_error("PostHog: Failed to prepare query: " + prepared_result.status().ToString());
	}

	auto prepared_statement = std::move(*prepared_result);

	// Get the result schema from the dataset schema.
	//
	// Important: explicitly Close() with call options. Arrow's PreparedStatement destructor
	// calls Close() with default FlightCallOptions (no headers), which breaks when the server
	// requires Authorization for ClosePreparedStatement.
	auto schema = prepared_statement->dataset_schema();
	auto close_status = prepared_statement->Close(call_options);
	if (!close_status.ok()) {
		std::cerr << "[PostHog] Warning: Failed to close prepared statement after schema inference: "
		          << close_status.ToString() << '\n';
	}
	return schema;
}

TransactionId PostHogFlightClient::BeginTransaction() {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();
	auto result = sql_client_->BeginTransaction(call_options);
	if (!result.ok()) {
		throw std::runtime_error("PostHog: BeginTransaction failed: " + result.status().ToString());
	}
	return result->transaction_id();
}

void PostHogFlightClient::CommitTransaction(const TransactionId &txn_id) {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();
	arrow::flight::sql::Transaction txn(txn_id);
	auto status = sql_client_->Commit(call_options, txn);
	if (!status.ok()) {
		throw std::runtime_error("PostHog: CommitTransaction failed: " + status.ToString());
	}
}

void PostHogFlightClient::RollbackTransaction(const TransactionId &txn_id) {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();
	arrow::flight::sql::Transaction txn(txn_id);
	auto status = sql_client_->Rollback(call_options, txn);
	if (!status.ok()) {
		throw std::runtime_error("PostHog: RollbackTransaction failed: " + status.ToString());
	}
}

std::vector<PostHogDbSchemaInfo> PostHogFlightClient::ListDbSchemas(const std::string &catalog) {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();

	// GetDbSchemas returns information about schemas/catalogs
	// Parameters: options, catalog (nullptr = all), db_schema_filter_pattern (nullptr = all)
	auto info_result = sql_client_->GetDbSchemas(call_options, catalog.empty() ? nullptr : &catalog, nullptr);
	if (!info_result.ok()) {
		throw std::runtime_error("PostHog: Failed to list db schemas: " + info_result.status().ToString());
	}

	std::vector<PostHogDbSchemaInfo> schemas;
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

		const auto &chunk = *chunk_result;
		if (!chunk.data) {
			break;
		}

		auto catalog_col = chunk.data->GetColumnByName("catalog_name");

		// RecordBatch::GetColumnByName returns Array, not ChunkedArray
		auto schema_col = chunk.data->GetColumnByName("db_schema_name");
		if (!schema_col) {
			schema_col = chunk.data->GetColumnByName("schema_name");
		}

		if (!schema_col) {
			continue;
		}

		auto read_string = [&](const std::shared_ptr<arrow::Array> &array, int64_t row) -> std::string {
			if (!array) {
				return "";
			}
			switch (array->type_id()) {
			case arrow::Type::STRING: {
				auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
				return str_array->IsNull(row) ? "" : std::string(str_array->GetView(row));
			}
			case arrow::Type::LARGE_STRING: {
				auto str_array = std::static_pointer_cast<arrow::LargeStringArray>(array);
				return str_array->IsNull(row) ? "" : std::string(str_array->GetView(row));
			}
			default:
				throw std::runtime_error("PostHog: Unexpected string column type: " + array->type()->ToString());
			}
		};

		for (int64_t i = 0; i < chunk.data->num_rows(); i++) {
			if (schema_col->IsNull(i)) {
				continue;
			}
			PostHogDbSchemaInfo entry;
			entry.catalog_name = read_string(catalog_col, i);
			entry.schema_name = read_string(schema_col, i);

			if (!catalog.empty() && catalog_col && entry.catalog_name != catalog) {
				continue;
			}
			schemas.push_back(std::move(entry));
		}
	}

	return schemas;
}

std::vector<std::string> PostHogFlightClient::ListTables(const std::string &catalog, const std::string &schema) {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();

	// GetTables returns information about tables
	// Parameters: catalog, schema_filter_pattern, table_name_filter_pattern, include_schema, table_types
	auto info_result =
	    sql_client_->GetTables(call_options, catalog.empty() ? nullptr : &catalog, &schema, nullptr, false, nullptr);
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

		const auto &chunk = *chunk_result;
		if (!chunk.data) {
			break;
		}

		auto catalog_col = chunk.data->GetColumnByName("catalog_name");
		auto table_col = chunk.data->GetColumnByName("table_name");
		if (table_col) {
			auto row_matches_catalog = [&](int64_t row) -> bool {
				if (catalog.empty() || !catalog_col) {
					return true;
				}
				switch (catalog_col->type_id()) {
				case arrow::Type::STRING: {
					auto catalog_array = std::static_pointer_cast<arrow::StringArray>(catalog_col);
					return !catalog_array->IsNull(row) && catalog_array->GetView(row) == catalog;
				}
				case arrow::Type::LARGE_STRING: {
					auto catalog_array = std::static_pointer_cast<arrow::LargeStringArray>(catalog_col);
					return !catalog_array->IsNull(row) && catalog_array->GetView(row) == catalog;
				}
				default:
					throw std::runtime_error("PostHog: Unexpected catalog_name column type: " +
					                         catalog_col->type()->ToString());
				}
			};

			switch (table_col->type_id()) {
			case arrow::Type::STRING: {
				auto table_array = std::static_pointer_cast<arrow::StringArray>(table_col);
				tables.reserve(tables.size() + static_cast<size_t>(table_array->length()));
				for (int64_t i = 0; i < table_array->length(); i++) {
					if (!row_matches_catalog(i) || table_array->IsNull(i)) {
						continue;
					}
					tables.emplace_back(table_array->GetView(i));
				}
				break;
			}
			case arrow::Type::LARGE_STRING: {
				auto table_array = std::static_pointer_cast<arrow::LargeStringArray>(table_col);
				tables.reserve(tables.size() + static_cast<size_t>(table_array->length()));
				for (int64_t i = 0; i < table_array->length(); i++) {
					if (!row_matches_catalog(i) || table_array->IsNull(i)) {
						continue;
					}
					tables.emplace_back(table_array->GetView(i));
				}
				break;
			}
			default:
				throw std::runtime_error("PostHog: Unexpected table_name column type: " +
				                         table_col->type()->ToString());
			}
		}
	}

	return tables;
}

std::shared_ptr<arrow::Schema>
PostHogFlightClient::GetTableSchema(const std::string &catalog, const std::string &schema, const std::string &table) {
	std::lock_guard<std::mutex> lock(client_mutex_);

	if (!authenticated_) {
		throw std::runtime_error("PostHog: Not authenticated. Call Authenticate() first.");
	}

	auto call_options = GetCallOptions();

	// GetTables with include_schema=true returns serialized schema in the result
	auto info_result =
	    sql_client_->GetTables(call_options, catalog.empty() ? nullptr : &catalog, &schema, &table, true, nullptr);
	if (!info_result.ok()) {
		throw std::runtime_error("PostHog: Failed to get table schema: " + info_result.status().ToString());
	}

	auto flight_info = std::move(*info_result);

	if (flight_info->endpoints().empty()) {
		throw std::runtime_error("PostHog: Table not found(endpoint empty): " + schema + "." + table);
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

	const auto &chunk = *chunk_result;
	if (!chunk.data || chunk.data->num_rows() == 0) {
		throw std::runtime_error("PostHog: Table not found(no data): " + schema + "." + table);
	}

	// The table_schema column contains the IPC-serialized Arrow schema
	// RecordBatch::GetColumnByName returns Array, not ChunkedArray
	auto schema_col = chunk.data->GetColumnByName("table_schema");
	if (!schema_col) {
		throw std::runtime_error("PostHog: Server did not return table schema for: " + schema + "." + table);
	}

	auto catalog_col = chunk.data->GetColumnByName("catalog_name");
	auto table_name_col = chunk.data->GetColumnByName("table_name");
	if (!table_name_col) {
		throw std::runtime_error("PostHog: Server did not return table_name column");
	}

	// Find the row matching the requested table name
	int64_t row_idx = -1;
	for (int64_t i = 0; i < chunk.data->num_rows(); i++) {
		if (!catalog.empty() && catalog_col) {
			switch (catalog_col->type_id()) {
			case arrow::Type::STRING: {
				auto catalog_array = std::static_pointer_cast<arrow::StringArray>(catalog_col);
				if (catalog_array->IsNull(i) || catalog_array->GetView(i) != catalog) {
					continue;
				}
				break;
			}
			case arrow::Type::LARGE_STRING: {
				auto catalog_array = std::static_pointer_cast<arrow::LargeStringArray>(catalog_col);
				if (catalog_array->IsNull(i) || catalog_array->GetView(i) != catalog) {
					continue;
				}
				break;
			}
			default:
				throw std::runtime_error("PostHog: Unexpected catalog_name column type: " +
				                         catalog_col->type()->ToString());
			}
		}

		switch (table_name_col->type_id()) {
		case arrow::Type::STRING: {
			auto table_name_array = std::static_pointer_cast<arrow::StringArray>(table_name_col);
			if (!table_name_array->IsNull(i) && table_name_array->GetView(i) == table) {
				row_idx = i;
			}
			break;
		}
		case arrow::Type::LARGE_STRING: {
			auto table_name_array = std::static_pointer_cast<arrow::LargeStringArray>(table_name_col);
			if (!table_name_array->IsNull(i) && table_name_array->GetView(i) == table) {
				row_idx = i;
			}
			break;
		}
		default:
			throw std::runtime_error("PostHog: Unexpected table_name column type: " +
			                         table_name_col->type()->ToString());
		}

		if (row_idx >= 0) {
			break;
		}
	}

	if (row_idx < 0) {
		throw std::runtime_error("PostHog: Table not found in metadata: " + schema + "." + table);
	}

	std::string_view schema_bytes;
	switch (schema_col->type_id()) {
	case arrow::Type::BINARY: {
		auto schema_array = std::static_pointer_cast<arrow::BinaryArray>(schema_col);
		if (schema_array->IsNull(row_idx)) {
			throw std::runtime_error("PostHog: Table schema is null for: " + schema + "." + table);
		}
		schema_bytes = schema_array->GetView(row_idx);
		break;
	}
	case arrow::Type::LARGE_BINARY: {
		auto schema_array = std::static_pointer_cast<arrow::LargeBinaryArray>(schema_col);
		if (schema_array->IsNull(row_idx)) {
			throw std::runtime_error("PostHog: Table schema is null for: " + schema + "." + table);
		}
		schema_bytes = schema_array->GetView(row_idx);
		break;
	}
	default:
		throw std::runtime_error("PostHog: Unexpected table_schema column type: " + schema_col->type()->ToString());
	}

	// Drain remaining chunks so this stream is fully consumed before the next
	// RPC on a single-connection Flight session.
	while (true) {
		auto next_chunk_result = stream->Next();
		if (!next_chunk_result.ok()) {
			throw std::runtime_error("PostHog: Failed to finish reading table metadata: " +
			                         next_chunk_result.status().ToString());
		}
		const auto &next_chunk = *next_chunk_result;
		if (!next_chunk.data) {
			break;
		}
	}

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
                                                   std::mutex &client_mutex, arrow::flight::FlightCallOptions options,
                                                   std::unique_ptr<arrow::flight::FlightInfo> info)
    : client_(client), client_mutex_(client_mutex), options_(std::move(options)), info_(std::move(info)) {
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
	std::lock_guard<std::mutex> lock(client_mutex_);
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
		arrow::ipc::DictionaryMemo memo;
		auto schema_result = info_->GetSchema(&memo);
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
		auto chunk = *chunk_result;
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
