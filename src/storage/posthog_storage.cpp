//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// storage/posthog_storage.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"

#include "storage/posthog_storage.hpp"
#include "storage/posthog_transaction_manager.hpp"
#include "catalog/posthog_catalog.hpp"
#include "utils/connection_string.hpp"

namespace duckdb {

static unique_ptr<Catalog> PostHogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                         AttachedDatabase &db, const string &name, AttachInfo &info,
                                         AttachOptions &attach_options) {
    // Parse the connection string
    auto config = ConnectionString::Parse(info.path);

    // Validate required parameters
    if (config.token.empty()) {
        throw InvalidInputException(
            "PostHog: Missing authentication token. Use: ATTACH 'hog:database?token=YOUR_TOKEN'");
    }

    // Use default endpoint if not specified
    if (config.endpoint.empty()) {
        config.endpoint = PostHogConnectionConfig::DEFAULT_ENDPOINT;
    }

    // For now (Milestone 2), we don't actually connect to the Flight server
    // Just create the catalog with the configuration
    return make_uniq<PostHogCatalog>(db, name, std::move(config));
}

static unique_ptr<TransactionManager> PostHogCreateTransactionManager(
    optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db, Catalog &catalog) {
    auto &posthog_catalog = catalog.Cast<PostHogCatalog>();
    return make_uniq<PostHogTransactionManager>(db, posthog_catalog);
}

PostHogStorageExtension::PostHogStorageExtension() {
    attach = PostHogAttach;
    create_transaction_manager = PostHogCreateTransactionManager;
}

} // namespace duckdb
