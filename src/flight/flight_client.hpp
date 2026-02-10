//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/flight_client.hpp
//
// Arrow Flight SQL client wrapper for remote query execution
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <cstddef>
#include <optional>

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <arrow/type.h>

namespace duckdb {

// Opaque bytes representing a Flight SQL TransactionId (no encoding assumptions).
using TransactionId = std::string;

struct PostHogDbSchemaInfo {
	std::string catalog_name;
	std::string schema_name;
};

class PostHogFlightQueryStream {
public:
	PostHogFlightQueryStream(arrow::flight::sql::FlightSqlClient &client, std::mutex &client_mutex,
	                         arrow::flight::FlightCallOptions options, std::unique_ptr<arrow::flight::FlightInfo> info);

	arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema();
	arrow::Result<arrow::flight::FlightStreamChunk> Next();

private:
	arrow::flight::sql::FlightSqlClient &client_;
	std::mutex &client_mutex_;
	arrow::flight::FlightCallOptions options_;
	std::unique_ptr<arrow::flight::FlightInfo> info_;
	std::unique_ptr<arrow::flight::FlightStreamReader> reader_;
	size_t endpoint_index_ = 0;
	std::shared_ptr<arrow::Schema> schema_;

	arrow::Status OpenReader();
};

class PostHogFlightClient {
public:
	PostHogFlightClient(const std::string &endpoint, const std::string &user, const std::string &password,
	                    bool tls_skip_verify);
	~PostHogFlightClient();

	// Prevent copying (Flight client is not copyable)
	PostHogFlightClient(const PostHogFlightClient &) = delete;
	PostHogFlightClient &operator=(const PostHogFlightClient &) = delete;

	// Disallow moving (due to mutex member)
	PostHogFlightClient(PostHogFlightClient &&) = delete;
	PostHogFlightClient &operator=(PostHogFlightClient &&) = delete;

	//===--------------------------------------------------------------------===//
	// Authentication
	//===--------------------------------------------------------------------===//

	// Authenticate with the server using username/password over TLS.
	void Authenticate();

	// Check if currently authenticated
	bool IsAuthenticated() const {
		return authenticated_;
	}

	//===--------------------------------------------------------------------===//
	// Query Execution
	//===--------------------------------------------------------------------===//

	// Execute a SQL query and return results as an Arrow Table
	std::shared_ptr<arrow::Table> ExecuteQuery(const std::string &sql);

	// Execute a SQL update/DDL statement (Flight SQL StatementUpdate).
	int64_t ExecuteUpdate(const std::string &sql, const std::optional<TransactionId> &txn_id = std::nullopt);

	// Execute a SQL query and return results as a streaming reader
	std::unique_ptr<PostHogFlightQueryStream>
	ExecuteQueryStream(const std::string &sql, const std::optional<TransactionId> &txn_id = std::nullopt);

	// Get the schema of a query without executing it (uses Prepare)
	std::shared_ptr<arrow::Schema> GetQuerySchema(const std::string &sql,
	                                              const std::optional<TransactionId> &txn_id = std::nullopt);

	//===--------------------------------------------------------------------===//
	// Transactions (Flight SQL BeginTransaction/EndTransaction)
	//===--------------------------------------------------------------------===//

	TransactionId BeginTransaction();
	void CommitTransaction(const TransactionId &txn_id);
	void RollbackTransaction(const TransactionId &txn_id);

	//===--------------------------------------------------------------------===//
	// Metadata Operations
	//===--------------------------------------------------------------------===//

	// Best-effort connectivity check (runs a lightweight Flight SQL RPC).
	// This is intended for logging/debugging; it does not change client state.
	arrow::Status Ping();

	// List all schemas and preserve remote catalog_name (Flight SQL GetDbSchemas response).
	// If catalog is non-empty, the results are filtered to that catalog.
	std::vector<PostHogDbSchemaInfo> ListDbSchemas(const std::string &catalog);

	// List all tables in a schema
	std::vector<std::string> ListTables(const std::string &catalog, const std::string &schema);

	// Get the schema of a specific table
	std::shared_ptr<arrow::Schema> GetTableSchema(const std::string &catalog, const std::string &schema,
	                                              const std::string &table);

	//===--------------------------------------------------------------------===//
	// Connection Info
	//===--------------------------------------------------------------------===//

	std::string GetEndpoint() const {
		return endpoint_;
	}

	// Check if the client is connected
	bool IsConnected() const {
		return sql_client_ != nullptr;
	}

private:
	std::string endpoint_;
	std::string user_;
	std::string password_;
	bool authenticated_ = false;

	// Arrow Flight clients
	std::unique_ptr<arrow::flight::sql::FlightSqlClient> sql_client_;

	// Mutex for thread safety
	mutable std::mutex client_mutex_;

	// Get call options with authentication headers
	arrow::flight::FlightCallOptions GetCallOptions() const;
};

} // namespace duckdb
