#define DUCKDB_EXTENSION_MAIN

#include "include/duckhog_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "storage/posthog_storage.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void DuckhogVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value("DuckHog DuckDB Extension v0.1.0"));
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Adds support for PostHog remote data access via hog: protocol");

	// Register the storage extension for "hog:" protocol
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.storage_extensions["hog"] = make_uniq<PostHogStorageExtension>();

	// Register a simple version function to verify the extension loads
	auto duckhog_version_func = ScalarFunction("duckhog_version", {}, LogicalType::VARCHAR, DuckhogVersionScalarFun);
	loader.RegisterFunction(duckhog_version_func);
}

void DuckhogExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DuckhogExtension::Name() {
	return "duckhog";
}

std::string DuckhogExtension::Version() const {
#ifdef EXT_VERSION_DUCKHOG
	return EXT_VERSION_DUCKHOG;
#else
	return "v0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckhog, loader) {
	duckdb::LoadInternal(loader);
}
}
