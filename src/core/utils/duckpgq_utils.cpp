#include "duckpgq/core/utils/duckpgq_utils.hpp"
#include "duckpgq/common.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"

#include "duckpgq/core/functions/table/describe_property_graph.hpp"
#include "duckpgq/core/functions/table/drop_property_graph.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckpgq {

namespace core {
// Function to get DuckPGQState from ClientContext
DuckPGQState * GetDuckPGQState(ClientContext &context) {
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID, "Registered DuckPGQ state not found");
  }
  return dynamic_cast<DuckPGQState*>(lookup->second.get());
}

// Utility function to transform a string to lowercase
std::string ToLowerCase(const std::string &input) {
  std::string result = input;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

// Function to get PropertyGraphInfo from DuckPGQState
CreatePropertyGraphInfo* GetPropertyGraphInfo(DuckPGQState *duckpgq_state, const std::string &pg_name) {
  auto property_graph = duckpgq_state->registered_property_graphs.find(pg_name);
  if (property_graph == duckpgq_state->registered_property_graphs.end()) {
    throw Exception(ExceptionType::INVALID, "Property graph " + pg_name + " not found");
  }
  return dynamic_cast<CreatePropertyGraphInfo*>(property_graph->second.get());
}

// Function to validate the source node and edge table
shared_ptr<PropertyGraphTable> ValidateSourceNodeAndEdgeTable(CreatePropertyGraphInfo *pg_info, const std::string &node_table, const std::string &edge_table) {
  auto source_node_pg_entry = pg_info->GetTable(node_table);
  if (!source_node_pg_entry->is_vertex_table) {
    throw Exception(ExceptionType::INVALID, node_table + " is an edge table, expected a vertex table");
  }
  auto edge_pg_entry = pg_info->GetTable(edge_table);
  if (edge_pg_entry->is_vertex_table) {
    throw Exception(ExceptionType::INVALID, edge_table + " is a vertex table, expected an edge table");
  }
  if (!edge_pg_entry->IsSourceTable(node_table)) {
    throw Exception(ExceptionType::INVALID, "Vertex table " + node_table + " is not a source of edge table " + edge_table);
  }
  return edge_pg_entry;
}

// Function to create the SELECT node
unique_ptr<SelectNode> CreateSelectNode(const shared_ptr<PropertyGraphTable> &edge_pg_entry, const string& function_name, const string& function_alias) {
  auto select_node = make_uniq<SelectNode>();
  std::vector<unique_ptr<ParsedExpression>> select_expression;

  select_expression.emplace_back(make_uniq<ColumnRefExpression>(edge_pg_entry->source_pk[0], edge_pg_entry->source_reference));

  auto cte_col_ref = make_uniq<ColumnRefExpression>("temp", "__x");

  vector<unique_ptr<ParsedExpression>> function_children;
  function_children.push_back(make_uniq<ConstantExpression>(Value::INTEGER(0)));
  function_children.push_back(make_uniq<ColumnRefExpression>("rowid", edge_pg_entry->source_reference));
  auto function = make_uniq<FunctionExpression>(function_name, std::move(function_children));

  std::vector<unique_ptr<ParsedExpression>> addition_children;
  addition_children.emplace_back(std::move(cte_col_ref));
  addition_children.emplace_back(std::move(function));

  auto addition_function = make_uniq<FunctionExpression>("add", std::move(addition_children));
  addition_function->alias = function_alias;
  select_expression.emplace_back(std::move(addition_function));
  select_node->select_list = std::move(select_expression);

  auto src_base_ref = make_uniq<BaseTableRef>();
  src_base_ref->table_name = edge_pg_entry->source_reference;

  auto temp_cte_select_subquery = CreateCountCTESubquery();

  auto cross_join_ref = make_uniq<JoinRef>(JoinRefType::CROSS);
  cross_join_ref->left = std::move(src_base_ref);
  cross_join_ref->right = std::move(temp_cte_select_subquery);

  select_node->from_table = std::move(cross_join_ref);

  return select_node;
}

unique_ptr<BaseTableRef> CreateBaseTableRef(const string &table_name, const string &alias) {
  auto base_table_ref = make_uniq<BaseTableRef>();
  base_table_ref->table_name = table_name;
  if (!alias.empty()) {
    base_table_ref->alias = alias;
  }
  return base_table_ref;
}

unique_ptr<ColumnRefExpression> CreateColumnRefExpression(const string &column_name, const string &table_name, const string& alias) {
  unique_ptr<ColumnRefExpression> column_ref;
  if (table_name.empty()) {
    column_ref = make_uniq<ColumnRefExpression>(column_name);
  } else  {
    column_ref = make_uniq<ColumnRefExpression>(column_name, table_name);
  }
  if (!alias.empty()) {
    column_ref->alias = alias;
  }
  return column_ref;
}
} // namespace core
} // namespace duckpgq