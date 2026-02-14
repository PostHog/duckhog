//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// utils/arrow_value.cpp
//
//===----------------------------------------------------------------------===//

#include "utils/arrow_value.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include <arrow/scalar.h>

namespace duckdb {

Value ArrowScalarToValue(const std::shared_ptr<arrow::Scalar> &scalar, const LogicalType &type) {
	if (!scalar || !scalar->is_valid) {
		return Value(type);
	}

	switch (type.id()) {
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(NumericCast<int32_t>(static_cast<arrow::Int32Scalar &>(*scalar).value));
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(NumericCast<int64_t>(static_cast<arrow::Int64Scalar &>(*scalar).value));
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericCast<int16_t>(static_cast<arrow::Int16Scalar &>(*scalar).value));
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericCast<int8_t>(static_cast<arrow::Int8Scalar &>(*scalar).value));
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericCast<uint8_t>(static_cast<arrow::UInt8Scalar &>(*scalar).value));
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericCast<uint16_t>(static_cast<arrow::UInt16Scalar &>(*scalar).value));
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericCast<uint32_t>(static_cast<arrow::UInt32Scalar &>(*scalar).value));
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(NumericCast<uint64_t>(static_cast<arrow::UInt64Scalar &>(*scalar).value));
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(static_cast<arrow::DoubleScalar &>(*scalar).value);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(static_cast<arrow::FloatScalar &>(*scalar).value);
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(static_cast<arrow::BooleanScalar &>(*scalar).value);
	case LogicalTypeId::VARCHAR:
		if (scalar->type->id() == arrow::Type::STRING) {
			return Value(static_cast<arrow::StringScalar &>(*scalar).value->ToString());
		}
		if (scalar->type->id() == arrow::Type::LARGE_STRING) {
			return Value(static_cast<arrow::LargeStringScalar &>(*scalar).value->ToString());
		}
		break;
	default:
		break;
	}

	throw NotImplementedException("PostHog: unsupported RETURNING value type conversion for %s", type.ToString());
}

} // namespace duckdb
