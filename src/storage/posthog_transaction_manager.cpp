//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// storage/posthog_transaction_manager.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/posthog_transaction_manager.hpp"

#include "catalog/posthog_catalog.hpp"
#include "duckdb/common/exception.hpp"

#include <cctype>

namespace duckdb {

namespace {

bool IsConnectionFailureMessage(const std::string &message) {
    std::string lower;
    lower.reserve(message.size());
    for (unsigned char ch : message) {
        lower.push_back(static_cast<char>(std::tolower(ch)));
    }
    return lower.find("failed to connect") != std::string::npos ||
           lower.find("connection refused") != std::string::npos ||
           lower.find("unavailable") != std::string::npos ||
           lower.find("timed out") != std::string::npos;
}

} // namespace

PostHogTransactionManager::PostHogTransactionManager(AttachedDatabase &db_p, PostHogCatalog *catalog)
    : TransactionManager(db_p), catalog_(catalog) {
}

Transaction &PostHogTransactionManager::StartTransaction(ClientContext &context) {
    lock_guard<mutex> guard(transaction_lock_);

    auto transaction = make_uniq<PostHogTransaction>(*this, context);
    if (catalog_ && catalog_->IsConnected()) {
        try {
            transaction->remote_txn_id = catalog_->GetFlightClient().BeginTransaction();
        } catch (const std::exception &e) {
            if (IsConnectionFailureMessage(e.what())) {
                throw CatalogException("PostHog: Not connected to remote server.");
            }
            throw;
        }
    }
    auto &result = *transaction;
    transactions_[result] = std::move(transaction);
    return result;
}

ErrorData PostHogTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
    lock_guard<mutex> guard(transaction_lock_);

    auto &txn = transaction.Cast<PostHogTransaction>();
    if (catalog_ && txn.remote_txn_id.has_value()) {
        try {
            catalog_->GetFlightClient().CommitTransaction(*txn.remote_txn_id);
        } catch (const std::exception &e) {
            transactions_.erase(transaction);
            return ErrorData(ExceptionType::CONNECTION, e.what());
        }
    }

    transactions_.erase(transaction);
    return ErrorData();
}

void PostHogTransactionManager::RollbackTransaction(Transaction &transaction) {
    lock_guard<mutex> guard(transaction_lock_);

    auto &txn = transaction.Cast<PostHogTransaction>();
    if (catalog_ && txn.remote_txn_id.has_value()) {
        try {
            catalog_->GetFlightClient().RollbackTransaction(*txn.remote_txn_id);
        } catch (const std::exception &) {
            // Best-effort rollback for MVP; propagate errors via the original statement failure paths.
        }
        // Ensure local caches are invalidated so rolled-back DDL does not linger.
        catalog_->RefreshSchemas();
    }

    transactions_.erase(transaction);
}

void PostHogTransactionManager::Checkpoint(ClientContext &context, bool force) {
    // Remote database - no local checkpoint needed
}

} // namespace duckdb
