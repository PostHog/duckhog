#include "catch.hpp"
#include "flight/flight_client.hpp"
#include "storage/posthog_storage.hpp"

#include <arrow/api.h>
#include <arrow/flight/server.h>
#include <arrow/flight/sql/server.h>
#include <arrow/ipc/writer.h>
#include <duckdb.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/storage/storage_extension.hpp>

#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>

using namespace duckdb;

namespace {

constexpr const char *TEST_CATALOG = "remote_catalog";
constexpr const char *TEST_SCHEMA = "metadata_schema";

struct ObservedGetTablesCall {
	bool do_get;
	bool include_schema;
	std::optional<std::string> catalog;
	std::optional<std::string> db_schema_filter_pattern;
	std::optional<std::string> table_name_filter_pattern;
};

arrow::Result<std::string> SerializeSchemaForFlightSqlMetadata(const std::shared_ptr<arrow::Schema> &schema) {
	auto serialized = arrow::ipc::SerializeSchema(*schema);
	if (!serialized.ok()) {
		return serialized.status();
	}
	return std::string(reinterpret_cast<const char *>((*serialized)->data()),
	                   static_cast<size_t>((*serialized)->size()));
}

arrow::Status FinishArray(arrow::ArrayBuilder &builder, std::shared_ptr<arrow::Array> &array) {
	return builder.Finish(&array);
}

class CountingTablesFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
public:
	explicit CountingTablesFlightSqlServer(size_t table_count) : table_count_(table_count) {
	}

	void SetMalformedSchemaTableIndex(std::optional<size_t> table_index) {
		malformed_schema_table_index_ = table_index;
	}

	void SetOmitIncludedSchemaColumn(bool omit) {
		omit_included_schema_column_ = omit;
	}

	void SetIncludeSiblingSchemaRows(bool include) {
		include_sibling_schema_rows_ = include;
	}

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
	GetFlightInfoTables(const arrow::flight::ServerCallContext & /*context*/,
	                    const arrow::flight::sql::GetTables &command,
	                    const arrow::flight::FlightDescriptor &descriptor) override {
		RecordCall(command, false);

		const auto &response_schema = command.include_schema
		                                  ? arrow::flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema()
		                                  : arrow::flight::sql::SqlSchema::GetTablesSchema();
		std::vector<arrow::flight::FlightEndpoint> endpoints;
		endpoints.emplace_back(arrow::flight::Ticket(descriptor.cmd), std::vector<arrow::flight::Location> {},
		                       std::nullopt, "");
		auto info = arrow::flight::FlightInfo::Make(*response_schema, descriptor, endpoints,
		                                            static_cast<int64_t>(MatchingTableCount(command)), -1);
		if (!info.ok()) {
			return info.status();
		}
		return std::make_unique<arrow::flight::FlightInfo>(std::move(*info));
	}

	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
	DoGetDbSchemas(const arrow::flight::ServerCallContext & /*context*/,
	               const arrow::flight::sql::GetDbSchemas &command) override {
		auto batch = BuildDbSchemasBatch(command);
		if (!batch.ok()) {
			return batch.status();
		}

		auto reader = arrow::RecordBatchReader::Make({*batch}, (*batch)->schema());
		if (!reader.ok()) {
			return reader.status();
		}
		return std::make_unique<arrow::flight::RecordBatchStream>(*reader);
	}

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
	GetFlightInfoSchemas(const arrow::flight::ServerCallContext & /*context*/,
	                     const arrow::flight::sql::GetDbSchemas &command,
	                     const arrow::flight::FlightDescriptor &descriptor) override {
		const auto &response_schema = arrow::flight::sql::SqlSchema::GetDbSchemasSchema();
		std::vector<arrow::flight::FlightEndpoint> endpoints;
		endpoints.emplace_back(arrow::flight::Ticket(descriptor.cmd), std::vector<arrow::flight::Location> {},
		                       std::nullopt, "");
		auto info = arrow::flight::FlightInfo::Make(*response_schema, descriptor, endpoints,
		                                            MatchingSchemaCount(command), -1);
		if (!info.ok()) {
			return info.status();
		}
		return std::make_unique<arrow::flight::FlightInfo>(std::move(*info));
	}

	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
	DoGetTables(const arrow::flight::ServerCallContext & /*context*/,
	            const arrow::flight::sql::GetTables &command) override {
		RecordCall(command, true);

		auto batch = BuildTablesBatch(command);
		if (!batch.ok()) {
			return batch.status();
		}

		auto reader = arrow::RecordBatchReader::Make({*batch}, (*batch)->schema());
		if (!reader.ok()) {
			return reader.status();
		}
		return std::make_unique<arrow::flight::RecordBatchStream>(*reader);
	}

	std::vector<ObservedGetTablesCall> Calls() const {
		std::lock_guard<std::mutex> lock(mutex_);
		return calls_;
	}

private:
	void RecordCall(const arrow::flight::sql::GetTables &command, bool do_get) {
		std::lock_guard<std::mutex> lock(mutex_);
		calls_.push_back({do_get, command.include_schema, command.catalog, command.db_schema_filter_pattern,
		                  command.table_name_filter_pattern});
	}

	size_t MatchingTableCount(const arrow::flight::sql::GetTables &command) const {
		if (command.catalog && *command.catalog != TEST_CATALOG) {
			return 0;
		}
		if (command.db_schema_filter_pattern && *command.db_schema_filter_pattern != TEST_SCHEMA) {
			return 0;
		}
		if (command.table_name_filter_pattern) {
			for (size_t table_idx = 0; table_idx < table_count_; table_idx++) {
				if (*command.table_name_filter_pattern == TableName(table_idx)) {
					return 1;
				}
			}
			return 0;
		}
		return table_count_ + (include_sibling_schema_rows_ ? table_count_ : 0);
	}

	std::string TableName(size_t index) const {
		return "metadata_table_" + std::to_string(index);
	}

	std::string SiblingSchemaName() const {
		return "metadataXschema";
	}

	int64_t MatchingSchemaCount(const arrow::flight::sql::GetDbSchemas &command) const {
		if (command.catalog && *command.catalog != TEST_CATALOG) {
			return 0;
		}
		if (command.db_schema_filter_pattern && *command.db_schema_filter_pattern != TEST_SCHEMA) {
			return 0;
		}
		return 1;
	}

	arrow::Result<std::shared_ptr<arrow::RecordBatch>>
	BuildDbSchemasBatch(const arrow::flight::sql::GetDbSchemas &command) const {
		const auto &response_schema = arrow::flight::sql::SqlSchema::GetDbSchemasSchema();
		arrow::StringBuilder catalog_builder;
		arrow::StringBuilder db_schema_builder;
		int64_t row_count = 0;

		if (MatchingSchemaCount(command) > 0) {
			auto status = catalog_builder.Append(TEST_CATALOG);
			if (!status.ok()) {
				return status;
			}
			status = db_schema_builder.Append(TEST_SCHEMA);
			if (!status.ok()) {
				return status;
			}
			row_count++;
		}

		std::shared_ptr<arrow::Array> catalog_array;
		auto status = FinishArray(catalog_builder, catalog_array);
		if (!status.ok()) {
			return status;
		}
		std::shared_ptr<arrow::Array> db_schema_array;
		status = FinishArray(db_schema_builder, db_schema_array);
		if (!status.ok()) {
			return status;
		}

		std::vector<std::shared_ptr<arrow::Array>> arrays {catalog_array, db_schema_array};
		return arrow::RecordBatch::Make(response_schema, row_count, std::move(arrays));
	}

	arrow::Result<std::shared_ptr<arrow::RecordBatch>>
	BuildTablesBatch(const arrow::flight::sql::GetTables &command) const {
		const bool include_schema_column = command.include_schema && !omit_included_schema_column_;
		const auto &response_schema = include_schema_column
		                                  ? arrow::flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema()
		                                  : arrow::flight::sql::SqlSchema::GetTablesSchema();
		arrow::StringBuilder catalog_builder;
		arrow::StringBuilder db_schema_builder;
		arrow::StringBuilder table_name_builder;
		arrow::StringBuilder table_type_builder;
		arrow::BinaryBuilder table_schema_builder;
		int64_t row_count = 0;

		auto append_table = [&](const std::string &schema_name, size_t table_idx) -> arrow::Status {
			auto table_name = TableName(table_idx);
			if (command.catalog && *command.catalog != TEST_CATALOG) {
				return arrow::Status::OK();
			}
			if (command.db_schema_filter_pattern && *command.db_schema_filter_pattern != TEST_SCHEMA) {
				return arrow::Status::OK();
			}
			if (command.table_name_filter_pattern && *command.table_name_filter_pattern != table_name) {
				return arrow::Status::OK();
			}

			auto status = catalog_builder.Append(TEST_CATALOG);
			if (!status.ok()) {
				return status;
			}
			status = db_schema_builder.Append(schema_name);
			if (!status.ok()) {
				return status;
			}
			status = table_name_builder.Append(table_name);
			if (!status.ok()) {
				return status;
			}
			status = table_type_builder.Append("TABLE");
			if (!status.ok()) {
				return status;
			}
			if (include_schema_column) {
				auto table_schema = arrow::schema({arrow::field("id", arrow::int64()),
				                                   arrow::field("payload_" + std::to_string(table_idx), arrow::utf8())});
				std::string schema_bytes;
				if (malformed_schema_table_index_ && *malformed_schema_table_index_ == table_idx) {
					schema_bytes = "not an arrow schema";
				} else {
					auto serialized_schema = SerializeSchemaForFlightSqlMetadata(table_schema);
					if (!serialized_schema.ok()) {
						return serialized_schema.status();
					}
					schema_bytes = *serialized_schema;
				}
				status = table_schema_builder.Append(reinterpret_cast<const uint8_t *>(schema_bytes.data()),
				                                     static_cast<int32_t>(schema_bytes.size()));
				if (!status.ok()) {
					return status;
				}
			}
			row_count++;
			return arrow::Status::OK();
		};

		if (include_sibling_schema_rows_) {
			for (size_t table_idx = 0; table_idx < table_count_; table_idx++) {
				auto status = append_table(SiblingSchemaName(), table_idx);
				if (!status.ok()) {
					return status;
				}
			}
		}
		for (size_t table_idx = 0; table_idx < table_count_; table_idx++) {
			auto status = append_table(TEST_SCHEMA, table_idx);
			if (!status.ok()) {
				return status;
			}
		}

		std::shared_ptr<arrow::Array> catalog_array;
		auto status = FinishArray(catalog_builder, catalog_array);
		if (!status.ok()) {
			return status;
		}
		std::shared_ptr<arrow::Array> db_schema_array;
		status = FinishArray(db_schema_builder, db_schema_array);
		if (!status.ok()) {
			return status;
		}
		std::shared_ptr<arrow::Array> table_name_array;
		status = FinishArray(table_name_builder, table_name_array);
		if (!status.ok()) {
			return status;
		}
		std::shared_ptr<arrow::Array> table_type_array;
		status = FinishArray(table_type_builder, table_type_array);
		if (!status.ok()) {
			return status;
		}

		std::vector<std::shared_ptr<arrow::Array>> arrays {catalog_array, db_schema_array, table_name_array,
		                                                   table_type_array};
		if (include_schema_column) {
			std::shared_ptr<arrow::Array> table_schema_array;
			status = FinishArray(table_schema_builder, table_schema_array);
			if (!status.ok()) {
				return status;
			}
			arrays.push_back(table_schema_array);
		}

		return arrow::RecordBatch::Make(response_schema, row_count, std::move(arrays));
	}

	size_t table_count_;
	std::optional<size_t> malformed_schema_table_index_;
	bool omit_included_schema_column_ = false;
	bool include_sibling_schema_rows_ = false;
	mutable std::mutex mutex_;
	std::vector<ObservedGetTablesCall> calls_;
};

class RunningFlightSqlServer {
public:
	explicit RunningFlightSqlServer(CountingTablesFlightSqlServer &server) : server_(server) {
		auto location = arrow::flight::Location::ForGrpcTcp("127.0.0.1", 0);
		REQUIRE(location.ok());
		arrow::flight::FlightServerOptions options(*location);
		auto init_status = server_.Init(options);
		REQUIRE(init_status.ok());
		serve_thread_ = std::thread([this]() { serve_status_ = server_.Serve(); });
	}

	~RunningFlightSqlServer() {
		if (!stopped_) {
			(void)server_.Shutdown();
			if (serve_thread_.joinable()) {
				serve_thread_.join();
			}
		}
	}

	std::string Endpoint() const {
		return "grpc+tcp://127.0.0.1:" + std::to_string(server_.port());
	}

	arrow::Status Stop() {
		stopped_ = true;
		auto shutdown_status = server_.Shutdown();
		if (serve_thread_.joinable()) {
			serve_thread_.join();
		}
		if (!shutdown_status.ok()) {
			return shutdown_status;
		}
		return serve_status_;
	}

private:
	CountingTablesFlightSqlServer &server_;
	std::thread serve_thread_;
	arrow::Status serve_status_ = arrow::Status::OK();
	bool stopped_ = false;
};

} // namespace

TEST_CASE("Flight table metadata schema IPC decoding round trips", "[duckhog][metadata]") {
	auto schema = arrow::schema({arrow::field("id", arrow::int64()), arrow::field("name", arrow::utf8())});
	auto serialized = arrow::ipc::SerializeSchema(*schema);
	REQUIRE(serialized.ok());

	auto schema_bytes = std::string_view(reinterpret_cast<const char *>((*serialized)->data()),
	                                     static_cast<size_t>((*serialized)->size()));
	auto decoded = DeserializeFlightSqlTableSchema(schema_bytes);

	REQUIRE(decoded.ok());
	REQUIRE((*decoded)->Equals(*schema));
}

TEST_CASE("Flight table metadata schema IPC decoding reports malformed bytes", "[duckhog][metadata]") {
	std::string invalid_schema_bytes = "not an arrow schema";
	auto decoded = DeserializeFlightSqlTableSchema(invalid_schema_bytes);

	REQUIRE_FALSE(decoded.ok());
}

TEST_CASE("PostHogFlightClient exposes batched table schema metadata", "[duckhog][metadata]") {
	using BatchedMetadataMethod =
	    std::vector<PostHogTableMetadata> (PostHogFlightClient::*)(const std::string &, const std::string &);
	BatchedMetadataMethod method = &PostHogFlightClient::ListTablesWithSchemas;
	(void)method;

	PostHogTableMetadata metadata;
	metadata.table_name = "sample_table";
	metadata.arrow_schema = arrow::schema({arrow::field("id", arrow::int64())});

	REQUIRE(metadata.table_name == "sample_table");
	REQUIRE(metadata.arrow_schema->num_fields() == 1);
	REQUIRE((std::is_same<decltype(metadata.arrow_schema), std::shared_ptr<arrow::Schema>>::value));
}

TEST_CASE("ListTablesWithSchemas uses one batched metadata stream for a schema", "[duckhog][metadata]") {
	CountingTablesFlightSqlServer server(4);
	RunningFlightSqlServer running_server(server);

	{
		PostHogFlightClient client(running_server.Endpoint(), "user", "password", true);
		client.Authenticate();

		auto metadata = client.ListTablesWithSchemas(TEST_CATALOG, TEST_SCHEMA);

		REQUIRE(metadata.size() == 4);
		for (size_t table_idx = 0; table_idx < metadata.size(); table_idx++) {
			REQUIRE(metadata[table_idx].table_name == "metadata_table_" + std::to_string(table_idx));
			REQUIRE(metadata[table_idx].arrow_schema->num_fields() == 2);
			REQUIRE(metadata[table_idx].arrow_schema->field(1)->name() == "payload_" + std::to_string(table_idx));
		}
	}

	auto stop_status = running_server.Stop();
	REQUIRE(stop_status.ok());

	auto calls = server.Calls();
	size_t get_flight_info_tables_calls = 0;
	size_t do_get_tables_calls = 0;
	for (const auto &call : calls) {
		if (call.do_get) {
			do_get_tables_calls++;
		} else {
			get_flight_info_tables_calls++;
		}
		REQUIRE(call.include_schema);
		REQUIRE(call.catalog);
		REQUIRE(*call.catalog == TEST_CATALOG);
		REQUIRE(call.db_schema_filter_pattern);
		REQUIRE(*call.db_schema_filter_pattern == TEST_SCHEMA);
		REQUIRE_FALSE(call.table_name_filter_pattern);
	}

	REQUIRE(get_flight_info_tables_calls == 1);
	REQUIRE(do_get_tables_calls == 1);
}

TEST_CASE("ListTablesWithSchemas exact-filters returned schema rows", "[duckhog][metadata]") {
	CountingTablesFlightSqlServer server(4);
	server.SetIncludeSiblingSchemaRows(true);
	RunningFlightSqlServer running_server(server);

	{
		PostHogFlightClient client(running_server.Endpoint(), "user", "password", true);
		client.Authenticate();

		auto metadata = client.ListTablesWithSchemas(TEST_CATALOG, TEST_SCHEMA);

		REQUIRE(metadata.size() == 4);
		for (size_t table_idx = 0; table_idx < metadata.size(); table_idx++) {
			REQUIRE(metadata[table_idx].table_name == "metadata_table_" + std::to_string(table_idx));
			REQUIRE(metadata[table_idx].arrow_schema->field(1)->name() == "payload_" + std::to_string(table_idx));
		}
	}

	auto stop_status = running_server.Stop();
	REQUIRE(stop_status.ok());
}

TEST_CASE("GetTableSchema exact-filters returned schema rows", "[duckhog][metadata]") {
	CountingTablesFlightSqlServer server(1);
	server.SetIncludeSiblingSchemaRows(true);
	RunningFlightSqlServer running_server(server);

	{
		PostHogFlightClient client(running_server.Endpoint(), "user", "password", true);
		client.Authenticate();

		auto schema = client.GetTableSchema(TEST_CATALOG, TEST_SCHEMA, "metadata_table_0");

		REQUIRE(schema->num_fields() == 2);
		REQUIRE(schema->field(1)->name() == "payload_0");
	}

	auto stop_status = running_server.Stop();
	REQUIRE(stop_status.ok());
}

TEST_CASE("Catalog table loading uses batched metadata instead of per-table schema RPCs", "[duckhog][metadata]") {
	CountingTablesFlightSqlServer server(4);
	RunningFlightSqlServer running_server(server);

	DBConfig config;
	StorageExtension::Register(config, "hog", make_shared_ptr<PostHogStorageExtension>());
	DuckDB db(nullptr, &config);
	Connection con(db);

	auto attach_sql = "ATTACH 'hog:remote_catalog?user=user&password=password&flight_server=" +
	                  running_server.Endpoint() + "&tls_skip_verify=true' AS remote";
	auto attach_result = con.Query(attach_sql);
	REQUIRE_FALSE(attach_result->HasError());

	auto metadata_result = con.Query("SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'remote' "
	                                 "AND schema_name = 'metadata_schema' "
	                                 "AND table_name LIKE 'metadata_table_%'");
	REQUIRE_FALSE(metadata_result->HasError());
	REQUIRE(metadata_result->GetValue(0, 0).GetValue<int64_t>() == 4);

	auto calls = server.Calls();
	size_t table_metadata_calls = 0;
	for (const auto &call : calls) {
		table_metadata_calls++;
		REQUIRE(call.include_schema);
		REQUIRE_FALSE(call.table_name_filter_pattern);
	}
	REQUIRE(table_metadata_calls > 0);

	auto stop_status = running_server.Stop();
	REQUIRE(stop_status.ok());
}

TEST_CASE("ListTablesWithSchemas fails the whole schema on malformed table_schema", "[duckhog][metadata]") {
	CountingTablesFlightSqlServer server(4);
	server.SetMalformedSchemaTableIndex(2);
	RunningFlightSqlServer running_server(server);

	{
		PostHogFlightClient client(running_server.Endpoint(), "user", "password", true);
		client.Authenticate();

		try {
			(void)client.ListTablesWithSchemas(TEST_CATALOG, TEST_SCHEMA);
			FAIL("ListTablesWithSchemas should fail when a table_schema row is malformed");
		} catch (const std::exception &ex) {
			std::string message = ex.what();
			REQUIRE(message.find("Failed to list tables with schemas") != std::string::npos);
			REQUIRE(message.find("Failed to deserialize table_schema") != std::string::npos);
			REQUIRE(message.find("metadata_schema.metadata_table_2") != std::string::npos);
		}
	}

	auto stop_status = running_server.Stop();
	REQUIRE(stop_status.ok());
}

TEST_CASE("GetTableSchema wraps malformed metadata errors after stream cleanup", "[duckhog][metadata]") {
	CountingTablesFlightSqlServer server(1);
	server.SetOmitIncludedSchemaColumn(true);
	RunningFlightSqlServer running_server(server);

	{
		PostHogFlightClient client(running_server.Endpoint(), "user", "password", true);
		client.Authenticate();

		try {
			(void)client.GetTableSchema(TEST_CATALOG, TEST_SCHEMA, "metadata_table_0");
			FAIL("GetTableSchema should fail when the metadata stream omits table_schema");
		} catch (const std::exception &ex) {
			std::string message = ex.what();
			REQUIRE(message.find("Failed to get table schema") != std::string::npos);
			REQUIRE(message.find("Server did not return table schema") != std::string::npos);
		}
	}

	auto stop_status = running_server.Stop();
	REQUIRE(stop_status.ok());
}
