//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// storage/posthog_transaction_manager.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"
#include "storage/posthog_transaction.hpp"

namespace duckdb {

class PostHogTransactionManager : public TransactionManager {
public:
    explicit PostHogTransactionManager(AttachedDatabase &db_p);

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;
    void Checkpoint(ClientContext &context, bool force = false) override;

private:
    mutex transaction_lock_;
    reference_map_t<Transaction, unique_ptr<PostHogTransaction>> transactions_;
};

} // namespace duckdb
