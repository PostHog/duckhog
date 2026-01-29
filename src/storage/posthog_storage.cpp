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

    // Determine connection mode:
    // 1. If flight_server= is specified, use direct connection (dev/testing bypass)
    // 2. Otherwise, use control plane (production path)
    if (!config.UseDirectFlightServer()) {
        // Production path: use control plane to get flight endpoint
        if (config.control_plane.empty()) {
            config.control_plane = PostHogConnectionConfig::DEFAULT_CONTROL_PLANE;
        }
        // TODO: Call control plane API to get flight_endpoint and session_token
        // For now, throw an error indicating control plane is not yet implemented
        throw InvalidInputException(
            "PostHog: Control plane integration not yet implemented. "
            "For development, use: ATTACH 'hog:database?token=TOKEN&flight_server=grpc://host:port'");
    }

    // Dev mode: direct flight server connection
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
