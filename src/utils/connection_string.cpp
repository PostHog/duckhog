//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// utils/connection_string.cpp
//
//===----------------------------------------------------------------------===//

#include "utils/connection_string.hpp"
#include <stdexcept>
#include <sstream>

namespace duckdb {

std::string ConnectionString::UrlDecode(const std::string &str) {
    std::string result;
    result.reserve(str.size());

    for (size_t i = 0; i < str.size(); i++) {
        if (str[i] == '%' && i + 2 < str.size()) {
            // Parse hex digits
            char hex[3] = {str[i + 1], str[i + 2], '\0'};
            char *end;
            long value = strtol(hex, &end, 16);
            if (end == hex + 2) {
                result += static_cast<char>(value);
                i += 2;
                continue;
            }
        } else if (str[i] == '+') {
            result += ' ';
            continue;
        }
        result += str[i];
    }

    return result;
}

PostHogConnectionConfig ConnectionString::Parse(const std::string &connection_string) {
    PostHogConnectionConfig config;

    std::string uri = connection_string;

    // Split database name and query parameters at '?'
    auto query_pos = uri.find('?');
    if (query_pos != std::string::npos) {
        config.database = uri.substr(0, query_pos);
        std::string query = uri.substr(query_pos + 1);

        // Parse query parameters (key=value&key=value...)
        std::string::size_type pos = 0;
        while (pos < query.size()) {
            // Find the next '&' or end of string
            auto amp_pos = query.find('&', pos);
            std::string param;
            if (amp_pos != std::string::npos) {
                param = query.substr(pos, amp_pos - pos);
                pos = amp_pos + 1;
            } else {
                param = query.substr(pos);
                pos = query.size();
            }

            // Split at '='
            auto eq_pos = param.find('=');
            if (eq_pos != std::string::npos) {
                std::string key = param.substr(0, eq_pos);
                std::string value = UrlDecode(param.substr(eq_pos + 1));

                if (key == "token") {
                    config.token = value;
                } else if (key == "endpoint") {
                    config.endpoint = value;
                } else {
                    config.options[key] = value;
                }
            }
        }
    } else {
        config.database = uri;
    }

    return config;
}

} // namespace duckdb
