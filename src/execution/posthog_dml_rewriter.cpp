//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_dml_rewriter.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_dml_rewriter.hpp"

#include "duckdb.h"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tokens.hpp"

#include <cctype>

namespace duckdb {

namespace {

bool CatalogIsUnset(const string &name) {
	return name.empty();
}

void RemoveTrailingSemicolon(string &sql) {
	while (!sql.empty() && (sql.back() == ';' || std::isspace(static_cast<unsigned char>(sql.back())))) {
		sql.pop_back();
	}
}

void RewriteTableRefCatalog(TableRef &table_ref, const string &attached_catalog, const string &remote_catalog);

void RewriteColumnRef(ColumnRefExpression &colref, const string &attached_catalog, const string &remote_catalog) {
	if (colref.column_names.size() < 3) {
		return;
	}

	auto &catalog = colref.column_names[0];
	if (CatalogIsUnset(catalog)) {
		return;
	}
	if (StringUtil::CIEquals(catalog, attached_catalog)) {
		catalog = remote_catalog;
		return;
	}
	if (!StringUtil::CIEquals(catalog, remote_catalog)) {
		throw BinderException("PostHog: explicit references to external catalogs are not supported in remote DML");
	}
}

void RewriteExpression(unique_ptr<ParsedExpression> &expr, const string &attached_catalog,
                       const string &remote_catalog) {
	if (!expr) {
		return;
	}
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(
	    *expr, [&](ColumnRefExpression &colref) { RewriteColumnRef(colref, attached_catalog, remote_catalog); });
	ParsedExpressionIterator::VisitExpressionMutable<SubqueryExpression>(*expr, [&](SubqueryExpression &subquery_expr) {
		if (!subquery_expr.subquery || !subquery_expr.subquery->node) {
			return;
		}
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *subquery_expr.subquery->node,
		    [&](unique_ptr<ParsedExpression> &child_expr) {
			    RewriteExpression(child_expr, attached_catalog, remote_catalog);
		    },
		    [&](TableRef &child_ref) { RewriteTableRefCatalog(child_ref, attached_catalog, remote_catalog); });
	});
}

void RewriteTableRefCatalog(TableRef &table_ref, const string &attached_catalog, const string &remote_catalog) {
	if (table_ref.type == TableReferenceType::BASE_TABLE) {
		auto &base_ref = table_ref.Cast<BaseTableRef>();
		if (!CatalogIsUnset(base_ref.catalog_name) && !StringUtil::CIEquals(base_ref.catalog_name, attached_catalog) &&
		    !StringUtil::CIEquals(base_ref.catalog_name, remote_catalog)) {
			throw BinderException("PostHog: explicit references to external catalogs are not supported in remote DML");
		}
		if (StringUtil::CIEquals(base_ref.catalog_name, attached_catalog)) {
			base_ref.catalog_name = remote_catalog;
		}
	}
}

void RewriteTableRef(unique_ptr<TableRef> &table_ref, const string &attached_catalog, const string &remote_catalog) {
	if (!table_ref) {
		return;
	}
	ParsedExpressionIterator::EnumerateTableRefChildren(
	    *table_ref,
	    [&](unique_ptr<ParsedExpression> &child) { RewriteExpression(child, attached_catalog, remote_catalog); },
	    [&](TableRef &child_ref) { RewriteTableRefCatalog(child_ref, attached_catalog, remote_catalog); });
}

} // namespace

// TRUNCATE TABLE also flows through this path: DuckDB's grammar (delete.y) desugars
// TRUNCATE into a PGDeleteStmt with no WHERE/USING/RETURNING/WITH clauses, so it arrives
// here as a plain unconditional DELETE and is forwarded to the remote server as such.
PostHogRewrittenDeleteSQL RewriteRemoteDeleteSQL(const string &query, const string &attached_catalog,
                                                 const string &remote_catalog) {
	Parser parser;
	parser.ParseQuery(query);

	DeleteStatement *delete_stmt = nullptr;
	for (auto &statement : parser.statements) {
		if (statement->type != StatementType::DELETE_STATEMENT) {
			throw NotImplementedException("PostHog: mixed statement batches are not supported for remote DELETE");
		}
		if (delete_stmt) {
			throw NotImplementedException("PostHog: expected exactly one DELETE statement in query batch");
		}
		delete_stmt = &statement->Cast<DeleteStatement>();
	}
	if (!delete_stmt) {
		throw NotImplementedException("PostHog: no DELETE statement found in query batch");
	}

	auto rewritten_stmt_holder = delete_stmt->Copy();
	auto &rewritten_stmt = rewritten_stmt_holder->Cast<DeleteStatement>();
	RewriteTableRef(rewritten_stmt.table, attached_catalog, remote_catalog);
	for (auto &using_clause : rewritten_stmt.using_clauses) {
		RewriteTableRef(using_clause, attached_catalog, remote_catalog);
	}
	RewriteExpression(rewritten_stmt.condition, attached_catalog, remote_catalog);
	for (auto &expr : rewritten_stmt.returning_list) {
		RewriteExpression(expr, attached_catalog, remote_catalog);
	}
	// TODO: Add support for CTE expressions, currently will fail on the remote side.

	PostHogRewrittenDeleteSQL result;
	result.has_returning_clause = !rewritten_stmt.returning_list.empty();

	rewritten_stmt.returning_list.clear();
	result.non_returning_sql = rewritten_stmt.ToString();
	RemoveTrailingSemicolon(result.non_returning_sql);

	// Route DELETE .. RETURNING through a SELECT wrapper so backends that
	// append LIMIT 0 for schema probing still parse the statement.
	result.returning_sql = "WITH __duckhog_deleted_rows AS (" + result.non_returning_sql +
	                       " RETURNING *) SELECT * FROM __duckhog_deleted_rows";

	return result;
}

PostHogRewrittenDeleteSQL RewriteRemoteDeleteSQL(ClientContext &context, const string &attached_catalog,
                                                 const string &remote_catalog) {
	return RewriteRemoteDeleteSQL(context.GetCurrentQuery(), attached_catalog, remote_catalog);
}

PostHogRewrittenUpdateSQL RewriteRemoteUpdateSQL(const string &query, const string &attached_catalog,
                                                 const string &remote_catalog) {
	Parser parser;
	parser.ParseQuery(query);

	UpdateStatement *update_stmt = nullptr;
	for (auto &statement : parser.statements) {
		if (statement->type != StatementType::UPDATE_STATEMENT) {
			throw NotImplementedException("PostHog: mixed statement batches are not supported for remote UPDATE");
		}
		if (update_stmt) {
			throw NotImplementedException("PostHog: expected exactly one UPDATE statement in query batch");
		}
		update_stmt = &statement->Cast<UpdateStatement>();
	}
	if (!update_stmt) {
		throw NotImplementedException("PostHog: no UPDATE statement found in query batch");
	}

	auto rewritten_stmt_holder = update_stmt->Copy();
	auto &rewritten_stmt = rewritten_stmt_holder->Cast<UpdateStatement>();
	RewriteTableRef(rewritten_stmt.table, attached_catalog, remote_catalog);
	RewriteTableRef(rewritten_stmt.from_table, attached_catalog, remote_catalog);
	if (rewritten_stmt.set_info) {
		for (auto &expr : rewritten_stmt.set_info->expressions) {
			RewriteExpression(expr, attached_catalog, remote_catalog);
		}
		RewriteExpression(rewritten_stmt.set_info->condition, attached_catalog, remote_catalog);
	}
	for (auto &expr : rewritten_stmt.returning_list) {
		RewriteExpression(expr, attached_catalog, remote_catalog);
	}
	// TODO: Add support for CTE expressions, currently will fail on the remote side.

	PostHogRewrittenUpdateSQL result;
	result.has_returning_clause = !rewritten_stmt.returning_list.empty();

	rewritten_stmt.returning_list.clear();
	result.non_returning_sql = rewritten_stmt.ToString();
	RemoveTrailingSemicolon(result.non_returning_sql);

	// Route UPDATE ... RETURNING through a SELECT wrapper so backends that
	// append LIMIT 0 for schema probing still parse the statement.
	result.returning_sql = "WITH __duckhog_updated_rows AS (" + result.non_returning_sql +
	                       " RETURNING *) SELECT * FROM __duckhog_updated_rows";

	return result;
}

PostHogRewrittenMergeSQL RewriteRemoteMergeSQL(const string &query, const string &attached_catalog,
                                               const string &remote_catalog) {
	Parser parser;
	parser.ParseQuery(query);

	MergeIntoStatement *merge_stmt = nullptr;
	for (auto &statement : parser.statements) {
		if (statement->type != StatementType::MERGE_INTO_STATEMENT) {
			throw NotImplementedException("PostHog: mixed statement batches are not supported for remote MERGE");
		}
		if (merge_stmt) {
			throw NotImplementedException("PostHog: expected exactly one MERGE statement in query batch");
		}
		merge_stmt = &statement->Cast<MergeIntoStatement>();
	}
	if (!merge_stmt) {
		throw NotImplementedException("PostHog: no MERGE statement found in query batch");
	}

	auto rewritten_stmt_holder = merge_stmt->Copy();
	auto &rewritten = rewritten_stmt_holder->Cast<MergeIntoStatement>();

	// Rewrite target and source table refs
	RewriteTableRef(rewritten.target, attached_catalog, remote_catalog);
	RewriteTableRef(rewritten.source, attached_catalog, remote_catalog);

	// Rewrite ON join condition
	RewriteExpression(rewritten.join_condition, attached_catalog, remote_catalog);

	// Rewrite expressions inside each action
	for (auto &[_, action_list] : rewritten.actions) {
		for (auto &action : action_list) {
			// Rewrite action condition (AND clause)
			RewriteExpression(action->condition, attached_catalog, remote_catalog);
			// Rewrite UPDATE SET expressions
			if (action->update_info) {
				for (auto &expr : action->update_info->expressions) {
					RewriteExpression(expr, attached_catalog, remote_catalog);
				}
				RewriteExpression(action->update_info->condition, attached_catalog, remote_catalog);
			}
			// Rewrite INSERT expressions
			for (auto &expr : action->expressions) {
				RewriteExpression(expr, attached_catalog, remote_catalog);
			}
		}
	}

	// Rewrite RETURNING expressions
	for (auto &expr : rewritten.returning_list) {
		RewriteExpression(expr, attached_catalog, remote_catalog);
	}

	PostHogRewrittenMergeSQL result;
	result.has_returning_clause = !rewritten.returning_list.empty();

	rewritten.returning_list.clear();
	result.non_returning_sql = rewritten.ToString();
	RemoveTrailingSemicolon(result.non_returning_sql);

	result.returning_sql = "WITH __duckhog_merged_rows AS (" + result.non_returning_sql +
	                       " RETURNING *) SELECT * FROM __duckhog_merged_rows";

	return result;
}

PostHogRewrittenMergeSQL RewriteRemoteMergeSQL(ClientContext &context, const string &attached_catalog,
                                               const string &remote_catalog) {
	return RewriteRemoteMergeSQL(context.GetCurrentQuery(), attached_catalog, remote_catalog);
}

string BuildRemoteCreateTableSQL(const CreateTableInfo &info, const string &attached_catalog,
                                 const string &remote_catalog) {
	auto copied = info.Copy();
	auto &create_info = copied->Cast<CreateTableInfo>();

	if (!CatalogIsUnset(create_info.catalog) && !StringUtil::CIEquals(create_info.catalog, attached_catalog) &&
	    !StringUtil::CIEquals(create_info.catalog, remote_catalog)) {
		throw BinderException("PostHog: explicit references to external catalogs are not supported in remote CTAS");
	}
	if (StringUtil::CIEquals(create_info.catalog, attached_catalog)) {
		create_info.catalog = remote_catalog;
	}

	// Clear the query — the binder has already resolved columns into the ColumnList,
	// so ToString() will emit pure CREATE TABLE DDL with column definitions.
	create_info.query.reset();

	return create_info.ToString();
}

PostHogRewrittenUpdateSQL RewriteRemoteUpdateSQL(ClientContext &context, const string &attached_catalog,
                                                 const string &remote_catalog) {
	return RewriteRemoteUpdateSQL(context.GetCurrentQuery(), attached_catalog, remote_catalog);
}

string BuildRemoteCreateViewSQL(const CreateViewInfo &info, const string &attached_catalog,
                                const string &remote_catalog) {
	auto copied = unique_ptr_cast<CreateInfo, CreateViewInfo>(info.Copy());

	if (!CatalogIsUnset(copied->catalog) && !StringUtil::CIEquals(copied->catalog, attached_catalog) &&
	    !StringUtil::CIEquals(copied->catalog, remote_catalog)) {
		throw BinderException(
		    "PostHog: explicit references to external catalogs are not supported in remote CREATE VIEW");
	}
	if (StringUtil::CIEquals(copied->catalog, attached_catalog)) {
		copied->catalog = remote_catalog;
	}

	// Rewrite catalog references inside the view's SELECT query.
	if (copied->query && copied->query->node) {
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *copied->query->node,
		    [&](unique_ptr<ParsedExpression> &child) { RewriteExpression(child, attached_catalog, remote_catalog); },
		    [&](TableRef &child_ref) { RewriteTableRefCatalog(child_ref, attached_catalog, remote_catalog); });
	}

	return copied->ToString();
}

} // namespace duckdb
