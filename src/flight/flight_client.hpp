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

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <arrow/type.h>

namespace duckdb {

class PostHogFlightClient {
public:
    PostHogFlightClient(const std::string &endpoint, const std::string &token);
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

    // Authenticate with the server using bearer token
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

    // Get the schema of a query without executing it (uses Prepare)
    std::shared_ptr<arrow::Schema> GetQuerySchema(const std::string &sql);

    //===--------------------------------------------------------------------===//
    // Metadata Operations
    //===--------------------------------------------------------------------===//

    // List all schemas (catalogs) in the remote database
    std::vector<std::string> ListSchemas();

    // List all tables in a schema
    std::vector<std::string> ListTables(const std::string &schema);

    // Get the schema of a specific table
    std::shared_ptr<arrow::Schema> GetTableSchema(const std::string &schema, const std::string &table);

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
    std::string token_;
    bool authenticated_ = false;

    // Arrow Flight clients
    std::unique_ptr<arrow::flight::sql::FlightSqlClient> sql_client_;

    // Mutex for thread safety
    mutable std::mutex client_mutex_;

    // Get call options with authentication headers
    arrow::flight::FlightCallOptions GetCallOptions() const;
};

} // namespace duckdb
