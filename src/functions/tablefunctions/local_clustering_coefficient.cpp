#include "duckpgq/functions/tablefunctions/local_clustering_coefficient.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckpgq_extension.hpp"

#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>

namespace duckdb {

unique_ptr<TableRef>
LocalClusteringCoefficientFunction::LocalClusteringCoefficientBindReplace(
    ClientContext &context, TableFunctionBindInput &input) {
  auto pg_name = StringValue::Get(input.inputs[0]);
  std::transform(pg_name.begin(), pg_name.end(), pg_name.begin(), ::tolower);
  auto node_table = StringValue::Get(input.inputs[1]);
  std::transform(node_table.begin(), node_table.end(), node_table.begin(), ::tolower);
  auto edge_table = StringValue::Get(input.inputs[2]);
  std::transform(edge_table.begin(), edge_table.end(), edge_table.begin(), ::tolower);

  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID,
                    "Registered DuckPGQ state not found");
  }
  const auto duckpgq_state = dynamic_cast<DuckPGQState *>(lookup->second.get());
  auto property_graph = duckpgq_state->registered_property_graphs.find(pg_name);
  if (property_graph == duckpgq_state->registered_property_graphs.end()) {
    throw Exception(ExceptionType::INVALID,
                    "Property graph " + pg_name + " not found");
  }
  auto pg_info =
      dynamic_cast<CreatePropertyGraphInfo *>(property_graph->second.get());
  auto source_node_pg_entry = pg_info->GetTable(node_table);
  auto edge_pg_entry = pg_info->GetTable(edge_table);
  if (edge_pg_entry->source_reference != node_table) {
    throw Exception(ExceptionType::INVALID,
                    "Vertex table " + node_table +
                        " is not a source of edge table " + edge_table);
  }

  auto select_node = make_uniq<SelectNode>();
  std::vector<unique_ptr<ParsedExpression>> select_expression;
  select_expression.emplace_back(make_uniq<ColumnRefExpression>(edge_pg_entry->source_pk[0], edge_pg_entry->source_reference));

  select_node->select_list = std::move(select_expression);
  auto src_base_ref = make_uniq<BaseTableRef>();
  src_base_ref->table_name = edge_pg_entry->source_reference;
  select_node->from_table = std::move(src_base_ref);

  auto subquery = make_uniq<SelectStatement>();
  subquery->node = std::move(select_node);

  auto result = make_uniq<SubqueryRef>(std::move(subquery));

  return result;
}

} // namespace duckdb
