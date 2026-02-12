//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// utils/connection_string.cpp
//
//===----------------------------------------------------------------------===//

#include "utils/connection_string.hpp"
#include <cstdint>
#include <stdexcept>
#include <sstream>

namespace duckdb {

std::string ConnectionString::UrlDecode(const std::string &str, bool plus_as_space) {
	std::string result;
	result.reserve(str.size());

	for (size_t i = 0; i < str.size(); i++) {
		if (str[i] == '%' && i + 2 < str.size()) {
			// Parse hex digits
			char hex[3] = {str[i + 1], str[i + 2], '\0'};
			char *end;
			int64_t value = strtol(hex, &end, 16);
			if (end == hex + 2) {
				result += static_cast<char>(value);
				i += 2;
				continue;
			}
		} else if (plus_as_space && str[i] == '+') {
			result += ' ';
			continue;
		}
		result += str[i];
	}

	return result;
}

PostHogConnectionConfig ConnectionString::Parse(const std::string &connection_string) {
	PostHogConnectionConfig config;

	const std::string &uri = connection_string;

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
				std::string raw_value = param.substr(eq_pos + 1);
				bool plus_as_space = !(key == "flight_server" || key == "endpoint");
				std::string value = UrlDecode(raw_value, plus_as_space);

				if (key == "user") {
					config.user = value;
				} else if (key == "password") {
					config.password = value;
				} else if (key == "flight_server") {
					config.flight_server = value;
				} else if (key == "endpoint") {
					// Backward-compatible alias.
					config.flight_server = value;
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
