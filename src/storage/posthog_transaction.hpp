//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// storage/posthog_transaction.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "flight/flight_client.hpp"

#include <optional>

namespace duckdb {

class PostHogTransaction : public Transaction {
public:
	PostHogTransaction(TransactionManager &manager, ClientContext &context) : Transaction(manager, context) {
	}

	// Remote Flight SQL transaction id for this DuckDB transaction (per attached hog: database).
	std::optional<TransactionId> remote_txn_id;

	static PostHogTransaction &Get(ClientContext &context, Catalog &catalog);
};

inline PostHogTransaction &PostHogTransaction::Get(ClientContext &context, Catalog &catalog) {
	auto &transaction = Transaction::Get(context, catalog);
	return transaction.Cast<PostHogTransaction>();
}

} // namespace duckdb
