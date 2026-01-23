# Arrow Conversion Refactor: C Bridge Approach

## Problem Statement

The current custom Arrow-to-DuckDB conversion code has a bug with decimal type handling:

```
INTERNAL Error: Expected vector of type INT64, but found vector of type INT16
```

This occurs because DuckDB maps `DECIMAL(precision, scale)` to different physical types based on precision:
- precision ≤ 4 → INT16
- precision ≤ 9 → INT32
- precision ≤ 18 → INT64
- precision ≤ 38 → INT128

The custom conversion code doesn't correctly handle this mapping.

## Current Architecture

```
Arrow Flight SQL Server
         │
         ▼
┌─────────────────────────┐
│  PostHogFlightClient    │  Returns arrow::Table (C++)
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│  ArrowConversion        │  Custom C++ conversion (BUGGY)
│  (arrow_conversion.cpp) │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│  DuckDB DataChunk       │
└─────────────────────────┘
```

### Issues with Current Approach

1. **Type handling bugs** - Decimal precision mapping is incorrect
2. **Maintenance burden** - Must manually handle all Arrow types
3. **Incomplete coverage** - Missing support for some nested types
4. **Duplicated effort** - DuckDB already has robust Arrow conversion

## Recommended Solution: C Bridge

Use Arrow's C Data Interface as a bridge to leverage DuckDB's built-in conversion.

### New Architecture

```
Arrow Flight SQL Server
         │
         ▼
┌─────────────────────────┐
│  PostHogFlightClient    │  Returns arrow::Table (C++)
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│  arrow::ExportArray()   │  Zero-copy export to C interface
│  arrow::ExportSchema()  │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│  ArrowArray / Schema    │  Arrow C Data Interface
│  (C structs)            │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│  DuckDB Arrow Scanner   │  Built-in, battle-tested conversion
│  ArrowToDuckDBConversion│
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│  DuckDB DataChunk       │
└─────────────────────────┘
```

### Benefits

| Aspect | Improvement |
|--------|-------------|
| Correctness | DuckDB's conversion handles all edge cases |
| Maintenance | No custom type handling code to maintain |
| Completeness | Automatic support for all Arrow types |
| Future-proof | New types supported automatically |

## Implementation Plan

### Understanding DuckDB's Arrow Conversion Requirements

DuckDB's `ArrowToDuckDB` function requires several properly initialized components:

1. **`ArrowScanLocalState`** - Contains `chunk` (an `ArrowArrayWrapper` with the C interface ArrowArray)
2. **`arrow_column_map_t`** - A map of column index → `ArrowType*` containing type metadata
3. **`ArrowType`** objects - Constructed from ArrowSchema via `ArrowType::GetArrowLogicalType()`

The conversion code at `duckdb/src/function/table/arrow_conversion.cpp:1400-1437` shows:
- It reads from `scan_state.chunk->arrow_array.children[idx]`
- It looks up `ArrowType` from `arrow_convert_data.at(col_idx)`
- It calls `ColumnArrowToDuckDB` with proper state

### Phase 1: Add C Bridge Utilities

Create utilities to export Arrow C++ objects to C interface and construct DuckDB ArrowType metadata.

**File: `src/flight/arrow_bridge.hpp`**

```cpp
#pragma once

#include <arrow/c/bridge.h>
#include <arrow/table.h>
#include <arrow/record_batch.h>

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {

class ArrowBridge {
public:
    //===--------------------------------------------------------------------===//
    // Export Arrow C++ to C Interface
    //===--------------------------------------------------------------------===//

    // Export Arrow C++ Schema to C interface
    static void ExportSchema(const std::shared_ptr<arrow::Schema> &schema,
                             ArrowSchema *out_schema);

    // Export Arrow C++ RecordBatch to C interface (includes schema)
    static void ExportRecordBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                                  ArrowArray *out_array,
                                  ArrowSchema *out_schema);

    //===--------------------------------------------------------------------===//
    // Build DuckDB ArrowType metadata from C interface schema
    //===--------------------------------------------------------------------===//

    // Populate ArrowTableSchema from exported C schema
    // This creates the ArrowType objects needed for conversion
    static void PopulateArrowTableSchema(ClientContext &context,
                                         ArrowTableSchema &arrow_table,
                                         const ArrowSchema &schema);

    //===--------------------------------------------------------------------===//
    // High-level conversion: Arrow C++ RecordBatch → DuckDB DataChunk
    //===--------------------------------------------------------------------===//

    // Convert an Arrow C++ RecordBatch directly to a DuckDB DataChunk
    // This handles all the bridging internally
    static void RecordBatchToDataChunk(ClientContext &context,
                                       const std::shared_ptr<arrow::RecordBatch> &batch,
                                       DataChunk &output,
                                       idx_t offset,
                                       idx_t count);
};

} // namespace duckdb
```

**File: `src/flight/arrow_bridge.cpp`**

```cpp
#include "flight/arrow_bridge.hpp"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void ArrowBridge::ExportSchema(const std::shared_ptr<arrow::Schema> &schema,
                               ArrowSchema *out_schema) {
    auto status = arrow::ExportSchema(*schema, out_schema);
    if (!status.ok()) {
        throw IOException("Failed to export Arrow schema: " + status.ToString());
    }
}

void ArrowBridge::ExportRecordBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                                    ArrowArray *out_array,
                                    ArrowSchema *out_schema) {
    auto status = arrow::ExportRecordBatch(*batch, out_array, out_schema);
    if (!status.ok()) {
        throw IOException("Failed to export Arrow record batch: " + status.ToString());
    }
}

void ArrowBridge::PopulateArrowTableSchema(ClientContext &context,
                                           ArrowTableSchema &arrow_table,
                                           const ArrowSchema &schema) {
    // Delegate to DuckDB's existing implementation
    ArrowTableFunction::PopulateArrowTableSchema(
        DBConfig::GetConfig(context),
        arrow_table,
        schema
    );
}

void ArrowBridge::RecordBatchToDataChunk(ClientContext &context,
                                         const std::shared_ptr<arrow::RecordBatch> &batch,
                                         DataChunk &output,
                                         idx_t offset,
                                         idx_t count) {
    // Step 1: Export to C interface
    ArrowArray c_array;
    ArrowSchema c_schema;
    ExportRecordBatch(batch, &c_array, &c_schema);

    // Step 2: Build ArrowType metadata
    ArrowTableSchema arrow_table;
    PopulateArrowTableSchema(context, arrow_table, c_schema);

    // Step 3: Create ArrowArrayWrapper to hold the C array
    auto chunk = make_shared_ptr<ArrowArrayWrapper>();
    chunk->arrow_array = c_array;
    // Note: Don't let c_array.release be called - ownership transferred to wrapper

    // Step 4: Create scan state
    ArrowScanLocalState scan_state(make_uniq<ArrowArrayWrapper>(), context);
    scan_state.chunk = chunk;
    scan_state.chunk_offset = offset;

    // Step 5: Build column_ids (all columns)
    for (idx_t i = 0; i < static_cast<idx_t>(c_schema.n_children); i++) {
        scan_state.column_ids.push_back(i);
    }

    // Step 6: Set output cardinality
    idx_t available = static_cast<idx_t>(batch->num_rows()) - offset;
    idx_t rows = MinValue<idx_t>(count, available);
    output.SetCardinality(rows);

    // Step 7: Convert using DuckDB's built-in conversion
    ArrowTableFunction::ArrowToDuckDB(
        scan_state,
        arrow_table.GetColumns(),  // arrow_column_map_t
        output,
        0  // start position within output chunk
    );

    // Clean up schema (array ownership was transferred)
    if (c_schema.release) {
        c_schema.release(&c_schema);
    }
}

} // namespace duckdb
```

### Phase 2: Modify Remote Scan to Use Bridge

**File: `src/catalog/remote_scan.cpp`**

Replace the custom conversion with the bridge.

```cpp
#include "flight/arrow_bridge.hpp"

void PostHogRemoteScan::Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<PostHogRemoteScanBindData>();
    auto &global_state = data.global_state->Cast<PostHogRemoteScanGlobalState>();

    // Execute the remote query if we haven't already
    if (!global_state.executed) {
        if (!bind_data.catalog.IsConnected()) {
            throw ConnectionException("PostHog: Not connected to remote server");
        }

        try {
            POSTHOG_LOG_DEBUG("Executing remote query: %s", bind_data.query.c_str());
            auto &client = bind_data.catalog.GetFlightClient();
            global_state.result_table = client.ExecuteQuery(bind_data.query);
            global_state.executed = true;

            // Convert table to record batches for streaming
            auto result = global_state.result_table->ToRecordBatches();
            if (!result.ok()) {
                throw IOException("Failed to convert to batches: " + result.status().ToString());
            }
            global_state.batches = std::move(result).ValueUnsafe();
            global_state.current_batch = 0;
            global_state.batch_offset = 0;

            POSTHOG_LOG_DEBUG("Query returned %lld rows in %zu batches",
                              global_state.result_table->num_rows(),
                              global_state.batches.size());
        } catch (const std::exception &e) {
            POSTHOG_LOG_ERROR("Failed to execute remote query: %s", e.what());
            throw IOException("PostHog: Failed to execute remote query: " + string(e.what()));
        }
    }

    // Check if we have more data
    if (global_state.current_batch >= global_state.batches.size()) {
        output.SetCardinality(0);
        return;
    }

    // Get current batch
    auto &batch = global_state.batches[global_state.current_batch];
    idx_t remaining_in_batch = static_cast<idx_t>(batch->num_rows()) - global_state.batch_offset;
    idx_t rows_to_copy = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_batch);

    // Use the bridge to convert
    ArrowBridge::RecordBatchToDataChunk(
        context,
        batch,
        output,
        global_state.batch_offset,
        rows_to_copy
    );

    // Advance position
    global_state.batch_offset += rows_to_copy;
    if (global_state.batch_offset >= static_cast<idx_t>(batch->num_rows())) {
        global_state.current_batch++;
        global_state.batch_offset = 0;
    }
}
```

### Phase 3: Update Global State

**File: `src/catalog/remote_scan.hpp`**

```cpp
struct PostHogRemoteScanGlobalState : public GlobalTableFunctionState {
    // Query execution state
    bool executed = false;
    std::shared_ptr<arrow::Table> result_table;

    // Batch iteration state
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    size_t current_batch = 0;
    idx_t batch_offset = 0;
};
```

### Phase 4: Remove Custom Conversion Code

After the new implementation is working, remove:

- `src/flight/arrow_conversion.cpp`
- `src/flight/arrow_conversion.hpp`

Update `CMakeLists.txt` to remove these files and add the new bridge files.

### Phase 5: Testing

1. Run existing tests to verify functionality:
   ```bash
   FLIGHT_HOST=127.0.0.1 FLIGHT_PORT=8815 ./build/release/test/unittest \
       --test-config test/configs/flight.json "test/sql/queries/*"
   ```

2. Add specific decimal type tests:
   ```sql
   -- Test various decimal precisions
   SELECT * FROM remote.main.decimal_test;
   ```

3. Test nested types (LIST, STRUCT, MAP) if applicable.

## Migration Checklist

- [ ] Create `src/flight/arrow_bridge.hpp`
- [ ] Create `src/flight/arrow_bridge.cpp`
- [ ] Update `CMakeLists.txt` to include new bridge files
- [ ] Add `#include <arrow/c/bridge.h>` dependency (should already be available via Arrow)
- [ ] Update `PostHogRemoteScanGlobalState` to store batches instead of raw table
- [ ] Update `PostHogRemoteScan::Execute` to use `ArrowBridge::RecordBatchToDataChunk`
- [ ] Run tests to verify decimal handling works
- [ ] Run full test suite to verify no regressions
- [ ] Test with various decimal precisions (1-4, 5-9, 10-18, 19-38)
- [ ] Remove `src/flight/arrow_conversion.cpp`
- [ ] Remove `src/flight/arrow_conversion.hpp`
- [ ] Update `CMakeLists.txt` to remove old conversion files

## Dependencies

The C bridge requires the Arrow C Data Interface header, which should already be available via the Arrow C++ library:

```cpp
#include <arrow/c/bridge.h>
```

No additional dependencies are needed.

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| DuckDB Arrow API changes | Low | Medium | Pin DuckDB version, test on upgrades |
| Performance regression | Low | Low | Benchmark before/after, C bridge is zero-copy |
| Missing type support | Very Low | Low | DuckDB's conversion is comprehensive |

## Conclusion

The C Bridge approach is recommended because it:

1. **Solves the immediate decimal bug** without manual type handling
2. **Reduces maintenance burden** by leveraging DuckDB's code
3. **Improves correctness** by using battle-tested conversion
4. **Future-proofs** the extension against new Arrow/DuckDB types

The implementation requires moderate effort but significantly improves code quality and reliability.
