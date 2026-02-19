//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// catalog/remote_table_function.hpp
//
// Proxy catalog-level table functions (e.g. snapshots()) through Flight SQL
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

#include <memory>

namespace duckdb {

class PostHogCatalog;
class SchemaCatalogEntry;

// Create a TableFunctionCatalogEntry that proxies a remote catalog-level
// table function through Flight SQL.  The function is invoked as a no-arg
// call: SELECT * FROM "remote_catalog"."function_name"().
unique_ptr<TableFunctionCatalogEntry>
CreateRemoteTableFunctionEntry(PostHogCatalog &catalog, SchemaCatalogEntry &schema, const string &function_name);

} // namespace duckdb
