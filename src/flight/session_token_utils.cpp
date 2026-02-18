//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/session_token_utils.cpp
//
// Utilities for Flight session-token header parsing and retryability checks
//===----------------------------------------------------------------------===//

#include "flight/session_token_utils.hpp"

#include "duckdb/common/string_util.hpp"

#include <cctype>

namespace duckdb {

namespace {
constexpr const char *kSessionHeader = "x-duckgres-session";
constexpr const char *kLegacySessionHeader = "x-duckgres-session-token";

bool HeaderNameEquals(std::string_view lhs, std::string_view rhs) {
	if (lhs.size() != rhs.size()) {
		return false;
	}
	for (size_t i = 0; i < lhs.size(); i++) {
		const auto left_ch = static_cast<unsigned char>(lhs[i]);
		const auto right_ch = static_cast<unsigned char>(rhs[i]);
		if (std::tolower(left_ch) != std::tolower(right_ch)) {
			return false;
		}
	}
	return true;
}

bool IsUnauthenticatedStatus(const arrow::Status &status) {
	auto detail = arrow::flight::FlightStatusDetail::UnwrapStatus(status);
	if (!detail) {
		return false;
	}
	return detail->code() == arrow::flight::FlightStatusCode::Unauthenticated ||
	       detail->code() == arrow::flight::FlightStatusCode::Unauthorized;
}
} // namespace

bool IsSessionTokenHeaderName(std::string_view header_name) {
	return HeaderNameEquals(header_name, kSessionHeader) || HeaderNameEquals(header_name, kLegacySessionHeader);
}

std::string ExtractSessionToken(const arrow::flight::CallHeaders &headers) {
	for (const auto &entry : headers) {
		if (!IsSessionTokenHeaderName(entry.first)) {
			continue;
		}
		std::string token(entry.second);
		StringUtil::Trim(token);
		if (!token.empty()) {
			return token;
		}
	}
	return "";
}

bool IsSessionTokenRetryableStatus(const arrow::Status &status) {
	if (IsUnauthenticatedStatus(status)) {
		return true;
	}

	auto lowered = StringUtil::Lower(status.ToString());
	return StringUtil::Contains(lowered, "x-duckgres-session") || StringUtil::Contains(lowered, "session token") ||
	       StringUtil::Contains(lowered, "session not found") || StringUtil::Contains(lowered, "transaction not found");
}

} // namespace duckdb
