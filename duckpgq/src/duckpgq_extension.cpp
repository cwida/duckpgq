#define DUCKDB_EXTENSION_MAIN

#include "duckpgq_extension.hpp"

#include <duckdb/parser/statement/insert_statement.hpp>

#include "duckdb/function/scalar_function.hpp"
#include "duckpgq/duckpgq_functions.hpp"

#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

#include "duckdb/parser/statement/extension_statement.hpp"

#include "duckpgq/functions/tablefunctions/drop_property_graph.hpp"
#include "duckpgq/functions/tablefunctions/create_property_graph.hpp"
#include "duckpgq/functions/tablefunctions/match.hpp"

namespace duckdb {

inline void DuckpgqScalarFun(DataChunk &args, ExpressionState &state,
                             Vector &result) {
  auto &name_vector = args.data[0];
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result,
                                       "Duckpgq " + name.GetString() + " 🐥");
      });
}

static void LoadInternal(DatabaseInstance &instance) {
  auto &config = DBConfig::GetConfig(instance);
  DuckPGQParserExtension pgq_parser;
  config.parser_extensions.push_back(pgq_parser);
  config.operator_extensions.push_back(make_uniq<DuckPGQOperatorExtension>());

  Connection con(instance);
  con.BeginTransaction();

  auto &catalog = Catalog::GetSystemCatalog(*con.context);

  PGQMatchFunction match_pg_function;
  CreateTableFunctionInfo match_pg_info(match_pg_function);
  catalog.CreateTableFunction(*con.context, match_pg_info);

  CreatePropertyGraphFunction create_pg_function;
  CreateTableFunctionInfo create_pg_info(create_pg_function);
  catalog.CreateTableFunction(*con.context, create_pg_info);

  DropPropertyGraphFunction drop_pg_function;
  CreateTableFunctionInfo drop_pg_info(drop_pg_function);
  catalog.CreateTableFunction(*con.context, drop_pg_info);

  for (auto &fun : DuckPGQFunctions::GetFunctions()) {
    catalog.CreateFunction(*con.context, fun);
  }

  for (auto &fun : DuckPGQFunctions::GetTableFunctions()) {
    catalog.CreateFunction(*con.context, fun);
  }

  CreateScalarFunctionInfo duckpgq_fun_info(
      ScalarFunction("duckpgq", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                     DuckpgqScalarFun));
  duckpgq_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
  catalog.CreateFunction(*con.context, duckpgq_fun_info);
  con.Commit();
}

void DuckpgqExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *info,
                                         const std::string &query) {
  auto parse_info = (DuckPGQParserExtensionInfo &)(info);
  Parser parser;
  parser.ParseQuery((query[0] == '-') ? query.substr(1, query.length())
                                      : query);
  if (parser.statements.size() != 1) {
    throw Exception(ExceptionType::PARSER,
        "More than 1 statement detected, please only give one.");
  }
  return {make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(
      std::move(parser.statements[0]))};
}

BoundStatement duckpgq_bind(ClientContext &context, Binder &binder,
                            OperatorExtensionInfo *info,
                            SQLStatement &statement) {
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::BINDER, "Registered state not found");
  }

  auto duckpgq_state = (DuckPGQState *)lookup->second.get();
  auto duckpgq_binder = Binder::CreateBinder(context);
  auto duckpgq_parse_data =
      dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());
  if (duckpgq_parse_data) {
    return duckpgq_binder->Bind(*(duckpgq_parse_data->statement));
  }
  throw Exception(ExceptionType::BINDER, "Unable to find DuckPGQ Parse Data");
}

void duckpgq_find_match_function(TableRef *table_ref,
                                 DuckPGQState &duckpgq_state) {
  if (auto table_function_ref = dynamic_cast<TableFunctionRef *>(table_ref)) {
    // Handle TableFunctionRef case
    auto function =
        dynamic_cast<FunctionExpression *>(table_function_ref->function.get());
    if (function->function_name == "duckpgq_match") {
      duckpgq_state.transform_expression =
          std::move(std::move(function->children[0]));
      function->children.pop_back();
    }
  } else if (auto join_ref = dynamic_cast<JoinRef *>(table_ref)) {
    // Handle JoinRef case
    duckpgq_find_match_function(join_ref->left.get(), duckpgq_state);
    duckpgq_find_match_function(join_ref->right.get(), duckpgq_state);
  }
}

ParserExtensionPlanResult
duckpgq_handle_statement(SQLStatement *statement, DuckPGQState &duckpgq_state) {
  if (statement->type == StatementType::SELECT_STATEMENT) {
    const auto select_statement = dynamic_cast<SelectStatement *>(statement);
    const auto select_node =
        dynamic_cast<SelectNode *>(select_statement->node.get());
    duckpgq_find_match_function(select_node->from_table.get(), duckpgq_state);
    throw Exception(ExceptionType::BINDER, "use duckpgq_bind instead");
  }
  if (statement->type == StatementType::CREATE_STATEMENT) {
    const auto &create_statement = statement->Cast<CreateStatement>();
    const auto create_property_graph =
        dynamic_cast<CreatePropertyGraphInfo *>(create_statement.info.get());
    if (create_property_graph) {
      ParserExtensionPlanResult result;
      result.function = CreatePropertyGraphFunction();
      result.requires_valid_transaction = true;
      result.return_type = StatementReturnType::QUERY_RESULT;
      return result;
    }
    const auto create_table =
        reinterpret_cast<CreateTableInfo *>(create_statement.info.get());
    duckpgq_handle_statement(create_table->query.get(), duckpgq_state);
  }
  if (statement->type == StatementType::DROP_STATEMENT) {
    ParserExtensionPlanResult result;
    result.function = DropPropertyGraphFunction();
    result.requires_valid_transaction = true;
    result.return_type = StatementReturnType::QUERY_RESULT;
    return result;
  }
  if (statement->type == StatementType::EXPLAIN_STATEMENT) {
    auto &explain_statement = statement->Cast<ExplainStatement>();
    // auto select_statement =
    // dynamic_cast<SelectStatement*>(explain_statement.stmt.get());
    duckpgq_handle_statement(explain_statement.stmt.get(), duckpgq_state);
  }
  if (statement->type == StatementType::COPY_STATEMENT) {
    const auto &copy_statement = statement->Cast<CopyStatement>();
    const auto select_node =
        dynamic_cast<SelectNode *>(copy_statement.select_statement.get());
    duckpgq_find_match_function(select_node->from_table.get(), duckpgq_state);
    throw Exception(ExceptionType::BINDER, "use duckpgq_bind instead");
  }
  if (statement->type == StatementType::INSERT_STATEMENT) {
    const auto &insert_statement = statement->Cast<InsertStatement>();
    duckpgq_handle_statement(insert_statement.select_statement.get(),
                             duckpgq_state);
  }

  // Preferably throw NotImplementedExpection here, but only BinderExceptions
  // are caught properly on MacOS right now
  throw Exception(ExceptionType::NOT_IMPLEMENTED,
    StatementTypeToString(statement->type) + "has not been implemented yet for DuckPGQ queries");
}

ParserExtensionPlanResult
duckpgq_plan(ParserExtensionInfo *, ClientContext &context,
             unique_ptr<ParserExtensionParseData> parse_data) {
  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  DuckPGQState *duckpgq_state;
  if (duckpgq_state_entry == context.registered_state.end()) {
    auto state = make_shared<DuckPGQState>(std::move(parse_data));
    context.registered_state["duckpgq"] = state;
    duckpgq_state = state.get();
  } else {
    duckpgq_state = (DuckPGQState *)duckpgq_state_entry->second.get();
    duckpgq_state->parse_data = std::move(parse_data);
  }
  auto duckpgq_parse_data =
      dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

  if (!duckpgq_parse_data) {
    throw Exception(ExceptionType::BINDER, "No DuckPGQ parse data found");
  }

  auto statement = duckpgq_parse_data->statement.get();
  return duckpgq_handle_statement(statement, *duckpgq_state);
}

std::string DuckpgqExtension::Name() { return "duckpgq"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckpgq_init(duckdb::DatabaseInstance &db) {
  LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *duckpgq_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
