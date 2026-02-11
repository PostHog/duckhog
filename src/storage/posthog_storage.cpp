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
#include "catalog/posthog_stub_catalog.hpp"
#include "utils/connection_string.hpp"
#include "flight/flight_client.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/string_util.hpp"
#include "utils/posthog_logger.hpp"

#include <set>

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

// Helper to enumerate all distinct catalog names from the remote Flight server
static std::vector<string> EnumerateRemoteCatalogs(PostHogFlightClient &client) {
	std::set<string> catalog_set;
	auto schema_infos = client.ListDbSchemas("");
	for (const auto &info : schema_infos) {
		if (!info.catalog_name.empty()) {
			catalog_set.insert(info.catalog_name);
		}
	}
	return std::vector<string>(catalog_set.begin(), catalog_set.end());
}

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

	// Check if this is a secondary catalog attachment (has __remote_catalog parameter)
	// If so, skip enumeration and just attach this specific catalog
	auto remote_catalog_it = config.options.find("__remote_catalog");
	if (remote_catalog_it != config.options.end()) {
		POSTHOG_LOG_INFO("Attaching secondary remote catalog '%s' as '%s'", remote_catalog_it->second.c_str(),
		                 name.c_str());
		return make_uniq<PostHogCatalog>(db, name, std::move(config), remote_catalog_it->second);
	}

	// If user specified a database/catalog in the connection string, use only that catalog
	if (!config.database.empty()) {
		string remote_catalog = config.database;
		POSTHOG_LOG_INFO("Attaching remote catalog '%s' as '%s'", remote_catalog.c_str(), name.c_str());
		return make_uniq<PostHogCatalog>(db, name, std::move(config), remote_catalog);
	}

	// No specific catalog requested: enumerate all remote catalogs and attach them
	std::vector<string> remote_catalogs;
	try {
		PostHogFlightClient temp_client(config.flight_server, config.user, config.password, config.tls_skip_verify);
		temp_client.Authenticate();
		remote_catalogs = EnumerateRemoteCatalogs(temp_client);
	} catch (const std::exception &e) {
		// If we can't connect, fall back to single catalog mode with empty remote_catalog
		POSTHOG_LOG_WARN("Failed to enumerate remote catalogs: %s. Using single catalog mode.", e.what());
		return make_uniq<PostHogCatalog>(db, name, std::move(config), "");
	}

	if (remote_catalogs.empty()) {
		// No catalogs found, use empty remote_catalog
		POSTHOG_LOG_WARN("No remote catalogs found. Using single catalog mode.");
		return make_uniq<PostHogCatalog>(db, name, std::move(config), "");
	}

	// Attach ALL remote catalogs with <name>_<catalog> naming convention
	auto &db_manager = DatabaseManager::Get(context);
	for (const string &remote_catalog : remote_catalogs) {
		string local_db_name = name + "_" + remote_catalog;

		// Check if this database is already attached
		if (db_manager.GetDatabase(context, local_db_name)) {
			POSTHOG_LOG_DEBUG("Database '%s' already attached, skipping.", local_db_name.c_str());
			continue;
		}

		POSTHOG_LOG_INFO("Attaching remote catalog '%s' as '%s'", remote_catalog.c_str(), local_db_name.c_str());

		// Create AttachInfo for the catalog
		AttachInfo additional_info;
		additional_info.path = info.path;
		// Add the remote_catalog as a query parameter
		if (additional_info.path.find('?') == string::npos) {
			additional_info.path += "?";
		} else {
			additional_info.path += "&";
		}
		additional_info.path += "__remote_catalog=" + remote_catalog;
		additional_info.name = local_db_name;

		// Create AttachOptions
		unordered_map<string, Value> opts;
		opts["type"] = Value("hog");
		AttachOptions additional_options(opts, attach_options.access_mode);

		try {
			auto attached_db = db_manager.AttachDatabase(context, additional_info, additional_options);
			if (!attached_db) {
				continue;
			}

			// DuckDB 1.4.x requires explicit initialize/finalize/finalize-attach when
			// using DatabaseManager::AttachDatabase directly.
			attached_db->Initialize(context);
			attached_db->FinalizeLoad(context);
			db_manager.FinalizeAttach(context, additional_info, std::move(attached_db));
		} catch (const std::exception &e) {
			POSTHOG_LOG_ERROR("Failed to attach catalog '%s': %s", remote_catalog.c_str(), e.what());
		}
	}

	// Return a stub catalog for the primary attachment (required by DuckDB)
	// The stub catalog has no tables - users should use the prefixed catalogs instead
	POSTHOG_LOG_INFO("Attaching stub catalog as '%s' (use '%s_<catalog>' for queries)", name.c_str(), name.c_str());
	return make_uniq<PostHogStubCatalog>(db, name);
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
