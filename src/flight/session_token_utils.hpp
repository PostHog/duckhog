//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/session_token_utils.hpp
//
// Utilities for Flight session-token header parsing and retryability checks
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <string_view>

#include <arrow/flight/api.h>

namespace duckdb {

bool IsSessionTokenHeaderName(std::string_view header_name);
std::string ExtractSessionToken(const arrow::flight::CallHeaders &headers);
bool IsSessionTokenRetryableStatus(const arrow::Status &status);

} // namespace duckdb
