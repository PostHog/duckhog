//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// storage/posthog_storage.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class PostHogStorageExtension : public StorageExtension {
public:
    PostHogStorageExtension();
};

} // namespace duckdb
