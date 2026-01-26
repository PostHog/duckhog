#define DUCKDB_EXTENSION_MAIN

#include "include/posthog_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "storage/posthog_storage.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void PosthogVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    result.SetValue(0, Value("PostHog DuckDB Extension v0.1.0"));
}

static void LoadInternal(ExtensionLoader &loader) {
    loader.SetDescription("Adds support for PostHog remote data access via hog: protocol");

    // Register the storage extension for "hog:" protocol
    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    StorageExtension::Register(config, "hog", make_shared_ptr<PostHogStorageExtension>());

    // Register a simple version function to verify the extension loads
    auto posthog_version_func = ScalarFunction("posthog_version", {}, LogicalType::VARCHAR, PosthogVersionScalarFun);
    loader.RegisterFunction(posthog_version_func);
}

void PosthogExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string PosthogExtension::Name() {
    return "posthog";
}

std::string PosthogExtension::Version() const {
#ifdef EXT_VERSION_POSTHOG
    return EXT_VERSION_POSTHOG;
#else
    return "v0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(posthog, loader) {
    duckdb::LoadInternal(loader);
}

}
