//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// utils/arrow_value.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"

#include <arrow/scalar.h>
#include <memory>

namespace duckdb {

Value ArrowScalarToValue(const std::shared_ptr<arrow::Scalar> &scalar, const LogicalType &type);

} // namespace duckdb
