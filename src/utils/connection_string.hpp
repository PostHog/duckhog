//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// utils/connection_string.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>

namespace duckdb {

struct PostHogConnectionConfig {
    std::string database;
    std::string token;
    std::string control_plane;
    // Direct Flight SQL server override (dev/testing only, bypasses control plane)
    std::string flight_server;
    std::unordered_map<std::string, std::string> options;

    // Default control plane endpoint (production path)
    static constexpr const char *DEFAULT_CONTROL_PLANE = "http://localhost:8080";

    // Check if using direct flight server bypass (dev mode)
    bool UseDirectFlightServer() const {
        return !flight_server.empty();
    }
};

class ConnectionString {
public:
    // Parse connection string format: "database_name?token=abc123&endpoint=grpc://host:port"
    // The "hog:" prefix is stripped by DuckDB before this is called
    static PostHogConnectionConfig Parse(const std::string &connection_string);

private:
    // URL decode a string (handles %XX encoding)
    static std::string UrlDecode(const std::string &str);
};

} // namespace duckdb
