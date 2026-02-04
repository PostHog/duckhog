//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// storage/posthog_transaction_manager.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/posthog_transaction_manager.hpp"

namespace duckdb {

PostHogTransactionManager::PostHogTransactionManager(AttachedDatabase &db_p)
    : TransactionManager(db_p) {
}

Transaction &PostHogTransactionManager::StartTransaction(ClientContext &context) {
    lock_guard<mutex> guard(transaction_lock_);

    auto transaction = make_uniq<PostHogTransaction>(*this, context);
    auto &result = *transaction;
    transactions_[result] = std::move(transaction);
    return result;
}

ErrorData PostHogTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
    lock_guard<mutex> guard(transaction_lock_);

    // For read-only operations, commit is a no-op
    transactions_.erase(transaction);
    return ErrorData();
}

void PostHogTransactionManager::RollbackTransaction(Transaction &transaction) {
    lock_guard<mutex> guard(transaction_lock_);

    // For read-only operations, rollback is a no-op
    transactions_.erase(transaction);
}

void PostHogTransactionManager::Checkpoint(ClientContext &context, bool force) {
    // Remote database - no local checkpoint needed
}

} // namespace duckdb
