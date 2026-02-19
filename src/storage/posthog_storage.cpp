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
#include "flight/flight_client.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/string_util.hpp"
#include "utils/posthog_logger.hpp"

namespace duckdb {

namespace {

bool ParseBoolOptionValue(const string &key, const string &value) {
	auto lower = StringUtil::Lower(value);
	if (lower == "true") {
		return true;
	}
	if (lower == "false") {
		return false;
	}
	throw InvalidInputException("PostHog: Invalid value for %s: '%s' (expected true or false).", key, value);
}

void ResolveSecurityOptions(PostHogConnectionConfig &config) {
	auto it = config.options.find("tls_skip_verify");
	if (it == config.options.end()) {
		return;
	}
	config.tls_skip_verify = ParseBoolOptionValue("tls_skip_verify", it->second);
	config.options.erase(it);
}

} // namespace

static unique_ptr<Catalog> PostHogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                         AttachedDatabase &db, const string &name, AttachInfo &info,
                                         AttachOptions &attach_options) {
	// Parse the connection string
	auto config = ConnectionString::Parse(info.path);

	if (config.user.empty()) {
		throw InvalidInputException(
		    "PostHog: Missing username. Use: ATTACH 'hog:database?user=USERNAME&password=PASSWORD'");
	}
	if (config.password.empty()) {
		throw InvalidInputException(
		    "PostHog: Missing password. Use: ATTACH 'hog:database?user=USERNAME&password=PASSWORD'");
	}

	if (config.flight_server.empty()) {
		config.flight_server = PostHogConnectionConfig::DEFAULT_FLIGHT_SERVER;
	}
	ResolveSecurityOptions(config);

	// Attach exactly one catalog.
	// If `config.database` is empty (e.g. hog:?user=...), the server resolves
	// the default catalog semantics.
	string remote_catalog = config.database;
	POSTHOG_LOG_INFO("Attaching remote catalog '%s' as '%s'", remote_catalog.c_str(), name.c_str());
	return make_uniq<PostHogCatalog>(db, name, std::move(config), remote_catalog);
}

static unique_ptr<TransactionManager> PostHogCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                      AttachedDatabase &db, Catalog &catalog) {
	auto *posthog_catalog = dynamic_cast<PostHogCatalog *>(&catalog);
	return make_uniq<PostHogTransactionManager>(db, posthog_catalog);
}

PostHogStorageExtension::PostHogStorageExtension() {
	attach = PostHogAttach;
	create_transaction_manager = PostHogCreateTransactionManager;
}

} // namespace duckdb
