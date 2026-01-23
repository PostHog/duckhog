//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// flight/arrow_conversion.hpp
//
// Type conversion between Arrow and DuckDB types
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"

#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/array.h>

namespace duckdb {

class ArrowConversion {
public:
    //===--------------------------------------------------------------------===//
    // Type Conversion
    //===--------------------------------------------------------------------===//

    // Convert Arrow data type to DuckDB LogicalType
    static LogicalType ArrowToDuckDB(const std::shared_ptr<arrow::DataType> &arrow_type);

    // Convert DuckDB LogicalType to Arrow data type
    static std::shared_ptr<arrow::DataType> DuckDBToArrow(const LogicalType &duckdb_type);

    //===--------------------------------------------------------------------===//
    // Schema Conversion
    //===--------------------------------------------------------------------===//

    // Convert Arrow schema to DuckDB column definitions (names and types)
    static void ArrowSchemaToDuckDB(const std::shared_ptr<arrow::Schema> &arrow_schema, vector<string> &names,
                                    vector<LogicalType> &types);

    //===--------------------------------------------------------------------===//
    // Data Conversion
    //===--------------------------------------------------------------------===//

    // Convert Arrow Table to DuckDB DataChunk
    // This is the main method for converting query results
    static void ArrowTableToDataChunk(const std::shared_ptr<arrow::Table> &table, DataChunk &output, idx_t start_row,
                                      idx_t count);

    // Convert a single Arrow Array to a DuckDB Vector
    static void ArrowArrayToVector(const std::shared_ptr<arrow::Array> &array, Vector &vector, idx_t start_row,
                                   idx_t count);

private:
    // Helper methods for specific type conversions
    static void ConvertBooleanArray(const std::shared_ptr<arrow::BooleanArray> &array, Vector &vector, idx_t start_row,
                                    idx_t count);

    template <typename ArrowType, typename DuckDBType>
    static void ConvertNumericArray(const std::shared_ptr<arrow::Array> &array, Vector &vector, idx_t start_row,
                                    idx_t count);

    static void ConvertStringArray(const std::shared_ptr<arrow::StringArray> &array, Vector &vector, idx_t start_row,
                                   idx_t count);

    static void ConvertLargeStringArray(const std::shared_ptr<arrow::LargeStringArray> &array, Vector &vector,
                                        idx_t start_row, idx_t count);

    static void ConvertBinaryArray(const std::shared_ptr<arrow::BinaryArray> &array, Vector &vector, idx_t start_row,
                                   idx_t count);

    static void ConvertTimestampArray(const std::shared_ptr<arrow::TimestampArray> &array, Vector &vector,
                                      idx_t start_row, idx_t count);

    static void ConvertDate32Array(const std::shared_ptr<arrow::Date32Array> &array, Vector &vector, idx_t start_row,
                                   idx_t count);

    static void ConvertDate64Array(const std::shared_ptr<arrow::Date64Array> &array, Vector &vector, idx_t start_row,
                                   idx_t count);

    static void ConvertDecimalArray(const std::shared_ptr<arrow::Decimal128Array> &array, Vector &vector,
                                    idx_t start_row, idx_t count, int32_t precision, int32_t scale);
};

} // namespace duckdb
