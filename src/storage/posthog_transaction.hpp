//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// storage/posthog_transaction.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class PostHogTransaction : public Transaction {
public:
    PostHogTransaction(TransactionManager &manager, ClientContext &context)
        : Transaction(manager, context) {
    }

    static PostHogTransaction &Get(ClientContext &context, Catalog &catalog);
};

} // namespace duckdb
