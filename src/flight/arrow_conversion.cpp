//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/arrow_conversion.cpp
//
// Type conversion between Arrow and DuckDB types
//===----------------------------------------------------------------------===//

#include "flight/arrow_conversion.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/type_traits.h>
#include <cstring>
#include <stdexcept>

namespace duckdb {

LogicalType ArrowConversion::ArrowToDuckDB(const std::shared_ptr<arrow::DataType> &arrow_type) {
    switch (arrow_type->id()) {
    // Boolean
    case arrow::Type::BOOL:
        return LogicalType::BOOLEAN;

    // Integers
    case arrow::Type::INT8:
        return LogicalType::TINYINT;
    case arrow::Type::INT16:
        return LogicalType::SMALLINT;
    case arrow::Type::INT32:
        return LogicalType::INTEGER;
    case arrow::Type::INT64:
        return LogicalType::BIGINT;

    // Unsigned integers
    case arrow::Type::UINT8:
        return LogicalType::UTINYINT;
    case arrow::Type::UINT16:
        return LogicalType::USMALLINT;
    case arrow::Type::UINT32:
        return LogicalType::UINTEGER;
    case arrow::Type::UINT64:
        return LogicalType::UBIGINT;

    // Floating point
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:
        return LogicalType::FLOAT;
    case arrow::Type::DOUBLE:
        return LogicalType::DOUBLE;

    // Decimal
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256: {
        auto decimal_type = std::static_pointer_cast<arrow::DecimalType>(arrow_type);
        return LogicalType::DECIMAL(decimal_type->precision(), decimal_type->scale());
    }

    // Strings
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
        return LogicalType::VARCHAR;

    // Binary
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
        return LogicalType::BLOB;

    // Date/Time types
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
        return LogicalType::DATE;

    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
        return LogicalType::TIME;

    case arrow::Type::TIMESTAMP: {
        auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arrow_type);
        // DuckDB uses microsecond precision for timestamps
        return LogicalType::TIMESTAMP;
    }

    case arrow::Type::INTERVAL_MONTHS:
    case arrow::Type::INTERVAL_DAY_TIME:
    case arrow::Type::INTERVAL_MONTH_DAY_NANO:
        return LogicalType::INTERVAL;

    // Null type
    case arrow::Type::NA:
        return LogicalType::SQLNULL;

    // List types
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST: {
        auto list_type = std::static_pointer_cast<arrow::BaseListType>(arrow_type);
        auto child_type = ArrowToDuckDB(list_type->value_type());
        return LogicalType::LIST(child_type);
    }

    // Struct type
    case arrow::Type::STRUCT: {
        auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow_type);
        child_list_t<LogicalType> children;
        for (int i = 0; i < struct_type->num_fields(); i++) {
            auto field = struct_type->field(i);
            children.push_back(make_pair(field->name(), ArrowToDuckDB(field->type())));
        }
        return LogicalType::STRUCT(std::move(children));
    }

    // Map type
    case arrow::Type::MAP: {
        auto map_type = std::static_pointer_cast<arrow::MapType>(arrow_type);
        auto key_type = ArrowToDuckDB(map_type->key_type());
        auto value_type = ArrowToDuckDB(map_type->item_type());
        return LogicalType::MAP(key_type, value_type);
    }

    default:
        throw std::runtime_error("PostHog: Unsupported Arrow type: " + arrow_type->ToString());
    }
}

std::shared_ptr<arrow::DataType> ArrowConversion::DuckDBToArrow(const LogicalType &duckdb_type) {
    switch (duckdb_type.id()) {
    case LogicalTypeId::BOOLEAN:
        return arrow::boolean();
    case LogicalTypeId::TINYINT:
        return arrow::int8();
    case LogicalTypeId::SMALLINT:
        return arrow::int16();
    case LogicalTypeId::INTEGER:
        return arrow::int32();
    case LogicalTypeId::BIGINT:
        return arrow::int64();
    case LogicalTypeId::UTINYINT:
        return arrow::uint8();
    case LogicalTypeId::USMALLINT:
        return arrow::uint16();
    case LogicalTypeId::UINTEGER:
        return arrow::uint32();
    case LogicalTypeId::UBIGINT:
        return arrow::uint64();
    case LogicalTypeId::FLOAT:
        return arrow::float32();
    case LogicalTypeId::DOUBLE:
        return arrow::float64();
    case LogicalTypeId::VARCHAR:
        return arrow::utf8();
    case LogicalTypeId::BLOB:
        return arrow::binary();
    case LogicalTypeId::DATE:
        return arrow::date32();
    case LogicalTypeId::TIME:
        return arrow::time64(arrow::TimeUnit::MICRO);
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_TZ:
        return arrow::timestamp(arrow::TimeUnit::MICRO);
    case LogicalTypeId::INTERVAL:
        return arrow::month_day_nano_interval();
    case LogicalTypeId::DECIMAL: {
        auto width = DecimalType::GetWidth(duckdb_type);
        auto scale = DecimalType::GetScale(duckdb_type);
        return arrow::decimal128(width, scale);
    }
    case LogicalTypeId::LIST: {
        auto child_type = DuckDBToArrow(ListType::GetChildType(duckdb_type));
        return arrow::list(child_type);
    }
    case LogicalTypeId::STRUCT: {
        auto &children = StructType::GetChildTypes(duckdb_type);
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (auto &child : children) {
            fields.push_back(arrow::field(child.first, DuckDBToArrow(child.second)));
        }
        return arrow::struct_(fields);
    }
    case LogicalTypeId::MAP: {
        auto key_type = DuckDBToArrow(MapType::KeyType(duckdb_type));
        auto value_type = DuckDBToArrow(MapType::ValueType(duckdb_type));
        return arrow::map(key_type, value_type);
    }
    case LogicalTypeId::HUGEINT:
        return arrow::decimal128(38, 0);
    default:
        throw std::runtime_error("PostHog: Unsupported DuckDB type: " + duckdb_type.ToString());
    }
}

void ArrowConversion::ArrowSchemaToDuckDB(const std::shared_ptr<arrow::Schema> &arrow_schema, vector<string> &names,
                                          vector<LogicalType> &types) {
    names.clear();
    types.clear();
    names.reserve(arrow_schema->num_fields());
    types.reserve(arrow_schema->num_fields());

    for (int i = 0; i < arrow_schema->num_fields(); i++) {
        auto field = arrow_schema->field(i);
        names.push_back(field->name());
        types.push_back(ArrowToDuckDB(field->type()));
    }
}

void ArrowConversion::ArrowTableToDataChunk(const std::shared_ptr<arrow::Table> &table, DataChunk &output,
                                            idx_t start_row, idx_t count) {
    D_ASSERT(output.ColumnCount() == static_cast<idx_t>(table->num_columns()));

    // Limit count to available rows
    if (start_row >= static_cast<idx_t>(table->num_rows())) {
        output.SetCardinality(0);
        return;
    }
    count = MinValue<idx_t>(count, static_cast<idx_t>(table->num_rows()) - start_row);

    // Convert each column
    for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
        auto chunked_array = table->column(col_idx);

        // Find the chunk and offset for start_row
        idx_t current_row = 0;
        for (int chunk_idx = 0; chunk_idx < chunked_array->num_chunks(); chunk_idx++) {
            auto chunk = chunked_array->chunk(chunk_idx);
            idx_t chunk_size = static_cast<idx_t>(chunk->length());

            if (current_row + chunk_size > start_row) {
                // This chunk contains our starting row
                idx_t chunk_start = start_row - current_row;
                idx_t chunk_count = MinValue<idx_t>(chunk_size - chunk_start, count);
                ArrowArrayToVector(chunk, output.data[col_idx], chunk_start, chunk_count);
                break;
            }
            current_row += chunk_size;
        }
    }

    output.SetCardinality(count);
}

void ArrowConversion::ArrowArrayToVector(const std::shared_ptr<arrow::Array> &array, Vector &vector, idx_t start_row,
                                         idx_t count) {
    switch (array->type_id()) {
    case arrow::Type::BOOL:
        ConvertBooleanArray(std::static_pointer_cast<arrow::BooleanArray>(array), vector, start_row, count);
        break;
    case arrow::Type::INT8:
        ConvertNumericArray<arrow::Int8Type, int8_t>(array, vector, start_row, count);
        break;
    case arrow::Type::INT16:
        ConvertNumericArray<arrow::Int16Type, int16_t>(array, vector, start_row, count);
        break;
    case arrow::Type::INT32:
        ConvertNumericArray<arrow::Int32Type, int32_t>(array, vector, start_row, count);
        break;
    case arrow::Type::INT64:
        ConvertNumericArray<arrow::Int64Type, int64_t>(array, vector, start_row, count);
        break;
    case arrow::Type::UINT8:
        ConvertNumericArray<arrow::UInt8Type, uint8_t>(array, vector, start_row, count);
        break;
    case arrow::Type::UINT16:
        ConvertNumericArray<arrow::UInt16Type, uint16_t>(array, vector, start_row, count);
        break;
    case arrow::Type::UINT32:
        ConvertNumericArray<arrow::UInt32Type, uint32_t>(array, vector, start_row, count);
        break;
    case arrow::Type::UINT64:
        ConvertNumericArray<arrow::UInt64Type, uint64_t>(array, vector, start_row, count);
        break;
    case arrow::Type::FLOAT:
        ConvertNumericArray<arrow::FloatType, float>(array, vector, start_row, count);
        break;
    case arrow::Type::DOUBLE:
        ConvertNumericArray<arrow::DoubleType, double>(array, vector, start_row, count);
        break;
    case arrow::Type::STRING:
        ConvertStringArray(std::static_pointer_cast<arrow::StringArray>(array), vector, start_row, count);
        break;
    case arrow::Type::LARGE_STRING:
        ConvertLargeStringArray(std::static_pointer_cast<arrow::LargeStringArray>(array), vector, start_row, count);
        break;
    case arrow::Type::BINARY:
        ConvertBinaryArray(std::static_pointer_cast<arrow::BinaryArray>(array), vector, start_row, count);
        break;
    case arrow::Type::TIMESTAMP:
        ConvertTimestampArray(std::static_pointer_cast<arrow::TimestampArray>(array), vector, start_row, count);
        break;
    case arrow::Type::DATE32:
        ConvertDate32Array(std::static_pointer_cast<arrow::Date32Array>(array), vector, start_row, count);
        break;
    case arrow::Type::DATE64:
        ConvertDate64Array(std::static_pointer_cast<arrow::Date64Array>(array), vector, start_row, count);
        break;
    case arrow::Type::DECIMAL128: {
        auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(array->type());
        ConvertDecimalArray(std::static_pointer_cast<arrow::Decimal128Array>(array), vector, start_row, count,
                            decimal_type->precision(), decimal_type->scale());
        break;
    }
    default:
        throw std::runtime_error("PostHog: Unsupported Arrow array type for conversion: " + array->type()->ToString());
    }
}

void ArrowConversion::ConvertBooleanArray(const std::shared_ptr<arrow::BooleanArray> &array, Vector &vector,
                                          idx_t start_row, idx_t count) {
    auto data = FlatVector::GetData<bool>(vector);
    auto &validity = FlatVector::Validity(vector);

    for (idx_t i = 0; i < count; i++) {
        idx_t arr_idx = start_row + i;
        if (array->IsNull(arr_idx)) {
            validity.SetInvalid(i);
        } else {
            data[i] = array->Value(arr_idx);
        }
    }
}

template <typename ArrowType, typename DuckDBType>
void ArrowConversion::ConvertNumericArray(const std::shared_ptr<arrow::Array> &array, Vector &vector, idx_t start_row,
                                          idx_t count) {
    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
    auto typed_array = std::static_pointer_cast<ArrayType>(array);

    auto data = FlatVector::GetData<DuckDBType>(vector);
    auto &validity = FlatVector::Validity(vector);

    for (idx_t i = 0; i < count; i++) {
        idx_t arr_idx = start_row + i;
        if (typed_array->IsNull(arr_idx)) {
            validity.SetInvalid(i);
        } else {
            data[i] = typed_array->Value(arr_idx);
        }
    }
}

void ArrowConversion::ConvertStringArray(const std::shared_ptr<arrow::StringArray> &array, Vector &vector,
                                         idx_t start_row, idx_t count) {
    auto &validity = FlatVector::Validity(vector);

    for (idx_t i = 0; i < count; i++) {
        idx_t arr_idx = start_row + i;
        if (array->IsNull(arr_idx)) {
            validity.SetInvalid(i);
        } else {
            auto str_view = array->GetView(arr_idx);
            FlatVector::GetData<string_t>(vector)[i] =
                StringVector::AddString(vector, str_view.data(), str_view.size());
        }
    }
}

void ArrowConversion::ConvertLargeStringArray(const std::shared_ptr<arrow::LargeStringArray> &array, Vector &vector,
                                              idx_t start_row, idx_t count) {
    auto &validity = FlatVector::Validity(vector);

    for (idx_t i = 0; i < count; i++) {
        idx_t arr_idx = start_row + i;
        if (array->IsNull(arr_idx)) {
            validity.SetInvalid(i);
        } else {
            auto str_view = array->GetView(arr_idx);
            FlatVector::GetData<string_t>(vector)[i] =
                StringVector::AddString(vector, str_view.data(), str_view.size());
        }
    }
}

void ArrowConversion::ConvertBinaryArray(const std::shared_ptr<arrow::BinaryArray> &array, Vector &vector,
                                         idx_t start_row, idx_t count) {
    auto &validity = FlatVector::Validity(vector);

    for (idx_t i = 0; i < count; i++) {
        idx_t arr_idx = start_row + i;
        if (array->IsNull(arr_idx)) {
            validity.SetInvalid(i);
        } else {
            auto bin_view = array->GetView(arr_idx);
            FlatVector::GetData<string_t>(vector)[i] =
                StringVector::AddStringOrBlob(vector, bin_view.data(), bin_view.size());
        }
    }
}

void ArrowConversion::ConvertTimestampArray(const std::shared_ptr<arrow::TimestampArray> &array, Vector &vector,
                                            idx_t start_row, idx_t count) {
    auto data = FlatVector::GetData<timestamp_t>(vector);
    auto &validity = FlatVector::Validity(vector);

    auto ts_type = std::static_pointer_cast<arrow::TimestampType>(array->type());
    auto unit = ts_type->unit();

    for (idx_t i = 0; i < count; i++) {
        idx_t arr_idx = start_row + i;
        if (array->IsNull(arr_idx)) {
            validity.SetInvalid(i);
        } else {
            int64_t value = array->Value(arr_idx);
            // Convert to microseconds (DuckDB's internal representation)
            switch (unit) {
            case arrow::TimeUnit::SECOND:
                data[i] = Timestamp::FromEpochSeconds(value);
                break;
            case arrow::TimeUnit::MILLI:
                data[i] = Timestamp::FromEpochMs(value);
                break;
            case arrow::TimeUnit::MICRO:
                data[i] = Timestamp::FromEpochMicroSeconds(value);
                break;
            case arrow::TimeUnit::NANO:
                data[i] = Timestamp::FromEpochNanoSeconds(value);
                break;
            }
        }
    }
}

void ArrowConversion::ConvertDate32Array(const std::shared_ptr<arrow::Date32Array> &array, Vector &vector,
                                         idx_t start_row, idx_t count) {
    auto data = FlatVector::GetData<date_t>(vector);
    auto &validity = FlatVector::Validity(vector);

    for (idx_t i = 0; i < count; i++) {
        idx_t arr_idx = start_row + i;
        if (array->IsNull(arr_idx)) {
            validity.SetInvalid(i);
        } else {
            // Arrow Date32 is days since Unix epoch, same as DuckDB
            data[i] = date_t(array->Value(arr_idx));
        }
    }
}

void ArrowConversion::ConvertDate64Array(const std::shared_ptr<arrow::Date64Array> &array, Vector &vector,
                                         idx_t start_row, idx_t count) {
    auto data = FlatVector::GetData<date_t>(vector);
    auto &validity = FlatVector::Validity(vector);

    for (idx_t i = 0; i < count; i++) {
        idx_t arr_idx = start_row + i;
        if (array->IsNull(arr_idx)) {
            validity.SetInvalid(i);
        } else {
            // Arrow Date64 is milliseconds since Unix epoch
            // Convert to days
            int64_t ms = array->Value(arr_idx);
            data[i] = date_t(static_cast<int32_t>(ms / (24 * 60 * 60 * 1000)));
        }
    }
}

void ArrowConversion::ConvertDecimalArray(const std::shared_ptr<arrow::Decimal128Array> &array, Vector &vector,
                                          idx_t start_row, idx_t count, int32_t precision, int32_t scale) {
    auto &validity = FlatVector::Validity(vector);

    // DuckDB stores decimals differently based on precision
    if (precision <= 18) {
        // Fits in int64_t
        auto data = FlatVector::GetData<int64_t>(vector);
        for (idx_t i = 0; i < count; i++) {
            idx_t arr_idx = start_row + i;
            if (array->IsNull(arr_idx)) {
                validity.SetInvalid(i);
            } else {
                // GetValue returns raw 16-byte little-endian representation
                // For values that fit in int64_t, just read the low 8 bytes
                const uint8_t *bytes = array->GetValue(arr_idx);
                int64_t value;
                memcpy(&value, bytes, sizeof(int64_t));
                data[i] = value;
            }
        }
    } else {
        // Needs hugeint
        auto data = FlatVector::GetData<hugeint_t>(vector);
        for (idx_t i = 0; i < count; i++) {
            idx_t arr_idx = start_row + i;
            if (array->IsNull(arr_idx)) {
                validity.SetInvalid(i);
            } else {
                // GetValue returns raw 16-byte little-endian representation
                // Low 8 bytes first, then high 8 bytes
                const uint8_t *bytes = array->GetValue(arr_idx);
                uint64_t low;
                int64_t high;
                memcpy(&low, bytes, sizeof(uint64_t));
                memcpy(&high, bytes + sizeof(uint64_t), sizeof(int64_t));
                data[i] = hugeint_t(high, low);
            }
        }
    }
}

} // namespace duckdb
