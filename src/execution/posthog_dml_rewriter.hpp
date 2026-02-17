//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_dml_rewriter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

namespace duckdb {

class ClientContext;

struct PostHogRewrittenUpdateSQL {
	std::string non_returning_sql;
	std::string returning_sql;
	bool has_returning_clause = false;
};

PostHogRewrittenUpdateSQL RewriteRemoteUpdateSQL(const std::string &query, const std::string &attached_catalog,
                                                 const std::string &remote_catalog);
PostHogRewrittenUpdateSQL RewriteRemoteUpdateSQL(ClientContext &context, const std::string &attached_catalog,
                                                 const std::string &remote_catalog);

struct PostHogRewrittenDeleteSQL {
	std::string non_returning_sql;
	std::string returning_sql;
	bool has_returning_clause = false;
};

PostHogRewrittenDeleteSQL RewriteRemoteDeleteSQL(const std::string &query, const std::string &attached_catalog,
                                                 const std::string &remote_catalog);
PostHogRewrittenDeleteSQL RewriteRemoteDeleteSQL(ClientContext &context, const std::string &attached_catalog,
                                                 const std::string &remote_catalog);

struct CreateTableInfo;

std::string BuildRemoteCreateTableSQL(const CreateTableInfo &info, const std::string &attached_catalog,
                                      const std::string &remote_catalog);

} // namespace duckdb
