# Arrow Conversion Refactor Implementation Plan

## Goals
- Replace custom Arrow-to-DuckDB conversion with DuckDB's Arrow scan path via Arrow C Data Interface.
- Enable true streaming ingestion (avoid materializing full Arrow tables).
- Preserve correct schema mapping for catalog entries.
- Fix decimal handling and improve type coverage (nested, dictionary, run-end encoded).

## Non-Goals (for this pass)
- New query pushdown features beyond what DuckDB's Arrow scan already supports.
- Changes to Flight SQL server behavior.
- Performance tuning beyond removing obvious materialization/copy steps.

## Decisions Needed Upfront
- Choose data path:
  - Preferred: RecordBatch stream -> ArrowArrayStream -> DuckDB Arrow scan.
  - Fallback: Table -> RecordBatches -> ArrowArrayStream (keeps materialization).
- Decide whether to use DuckDB C++ Arrow scan (current internal API) or C API entrypoints.
- Decide if any types need a custom fallback converter (hybrid approach).

## Implementation Phases

### Phase 1: Remove Custom Converter (Clean Slate) ✅
1. Remove files and references:
   - Delete `arrow_conversion.cpp` and `arrow_conversion.hpp`.
   - Update `CMakeLists.txt` and any includes.
2. Remove or replace any remaining call sites:
   - `ArrowSchemaToDuckDB` in catalog.
   - `ArrowTableToDataChunk` in `remote_scan`.
3. Confirm no unused code remains:
   - Build to verify no missing symbols.

### Phase 2: Schema Mapping for Catalog ✅
1. Replace usage in `PostHogSchemaEntry`:
   - Use DuckDB Arrow schema utilities to derive names/types.
2. Implement a helper function:
   - Accept `arrow::Schema` and return names/types via `PopulateArrowTableSchema`.
3. Validate schema parity:
   - Compare old vs. new type mappings for known columns (especially decimals).

### Phase 3: Streaming Bridge ✅
1. Define a Flight stream wrapper type:
   - Store `arrow::flight::FlightStreamReader` (or equivalent) and last-error state.
   - Track whether the stream has ended to return a released `ArrowArray`.
2. Implement `get_schema`:
   - Read the Arrow schema from the Flight stream on first use.
   - Export schema via `arrow::ExportSchema`.
3. Implement `get_next`:
   - Read the next RecordBatch from Flight.
   - Export RecordBatch via `arrow::ExportRecordBatch`.
   - Return `out->release = nullptr` on end-of-stream.
4. Implement `get_last_error`:
   - Store Arrow status error strings on failure and expose them.
5. Implement `release`:
   - Free wrapper state and clear function pointers.
6. Add RAII wrapper:
   - Wrap `ArrowArrayStream` in DuckDB's `ArrowArrayStreamWrapper`.
   - Ensure the Flight stream stays alive while DuckDB consumes it.

### Phase 4: Wire DuckDB Arrow Scan Correctly ✅
1. Build Arrow scan bind inputs:
   - Provide a stream factory pointer + produce/get_schema callbacks expected by `ArrowScanBind`.
   - Create any needed `ExternalDependency` ownership to keep the stream alive.
2. Call `ArrowScanBind` to generate `ArrowScanFunctionData`:
   - Validate names/types from `ArrowTableFunction::PopulateArrowTableSchema`.
3. Initialize global and local scan state:
   - Call `ArrowScanInitGlobal` and `ArrowScanInitLocal` with proper `TableFunctionInitInput`.
   - Ensure projection/filter fields are wired if used.
4. Execute scan via `ArrowScanFunction`:
   - Use DuckDB's scan function to fill `DataChunk`.
   - Remove manual row offset management; rely on scan state.
5. Threading and cleanup:
   - Respect `ArrowScanMaxThreads` if concurrency is introduced.
   - Ensure `ArrowArrayStream` is released when scan completes.

### Phase 5: Tests and Verification
1. Add decimal coverage:
   - Include precisions mapping to INT16/INT32/INT64/INT128.
2. Add nested type coverage:
   - LIST, STRUCT, MAP (including nulls and empty collections).
3. Add encoding coverage:
   - Dictionary encoded and run-end encoded arrays if available.
4. Run Flight integration tests:
   - Validate correctness and compare to baseline results.
5. Memory/perf sanity checks:
   - Verify no full-table materialization in the hot path.

## Risks and Mitigations
- API mismatch with DuckDB Arrow scan: pin DuckDB version and add compilation guards.
- Memory spikes if still materializing: verify streaming path in Flight client.
- Incomplete type coverage: add explicit tests and a fallback path if needed.

## Deliverables Checklist
- [x] `ArrowArrayStream` bridge from Flight RecordBatch stream
- [x] Proper DuckDB Arrow scan bind/init integration
- [x] Catalog schema mapping via DuckDB Arrow schema utilities
- [x] Removal of custom conversion code
- [ ] Test coverage for decimals and nested types
