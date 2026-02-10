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
    std::string user;
    std::string password;
    // Direct Flight SQL server endpoint.
    std::string flight_server;
    // If true, skip TLS certificate verification (for local/dev only).
    bool tls_skip_verify = false;
    std::unordered_map<std::string, std::string> options;

    static constexpr const char *DEFAULT_FLIGHT_SERVER = "grpc+tls://127.0.0.1:8815";
};

class ConnectionString {
public:
    // Parse connection string format:
    // "database_name?user=postgres&password=postgres&flight_server=grpc+tls://host:port&tls_skip_verify=true"
    // The "hog:" prefix is stripped by DuckDB before this is called
    static PostHogConnectionConfig Parse(const std::string &connection_string);

private:
    // URL decode a string (handles %XX encoding)
    static std::string UrlDecode(const std::string &str, bool plus_as_space);
};

} // namespace duckdb
