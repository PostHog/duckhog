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
#include "http/control_plane_client.hpp"

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

        // Call control plane API to get flight_endpoint and session_token
        auto cp_response = posthog::ControlPlaneClient::CreateSession(config.control_plane, config.token, config.database);

        // Update config with the returned flight endpoint
        config.flight_server = cp_response.flight_endpoint;

        // If control plane returns a session token, use it instead of the original API token
        if (cp_response.session_token.has_value()) {
            config.token = cp_response.session_token.value();
        }
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
