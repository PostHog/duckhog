//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// execution/posthog_dml_rewriter.cpp
//
//===----------------------------------------------------------------------===//

#include "execution/posthog_dml_rewriter.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include <cctype>

namespace duckdb {

namespace {

bool CatalogIsUnset(const string &name) {
	return name.empty() || name == INVALID_CATALOG;
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
		throw BinderException("PostHog: explicit references to external catalogs are not supported in remote UPDATE");
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
			throw BinderException(
			    "PostHog: explicit references to external catalogs are not supported in remote UPDATE");
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

PostHogRewrittenUpdateSQL RewriteRemoteUpdateSQL(ClientContext &context, const string &attached_catalog,
                                                 const string &remote_catalog) {
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(context.GetCurrentQuery());

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

} // namespace duckdb
