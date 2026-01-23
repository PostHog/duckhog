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
    std::string endpoint;
    std::unordered_map<std::string, std::string> options;

    // Default Flight SQL endpoint
    static constexpr const char *DEFAULT_ENDPOINT = "grpc://localhost:8815";
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
