#include "property_graphs_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

TableCatalogEntry &PropertyGraphsTable::GetPropertyGraphTable(ClientContext &context, const string &catalog_name) const {
  auto &catalog = Catalog::GetCatalog(context, catalog_name);
  auto &table_entry = catalog.GetEntry<TableCatalogEntry>(context, schema, table_name);
  return table_entry;
}

shared_ptr<PropertyGraphsTable> PropertyGraphsTable::GetOrCreate(ClientContext &context, const string &table_name, const string &catalog_name) {
  auto key = "PROPERTY_GRAPH_TABLE_CACHE_ENTRY_" + StringUtil::Upper(table_name);
  auto &cache = ObjectCache::GetObjectCache(context);
  auto &catalog = Catalog::GetCatalog(context, catalog_name);

  auto property_graphs_table_exist = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, DEFAULT_SCHEMA, table_name,
                                             OnEntryNotFound::RETURN_NULL) != nullptr;

  if (property_graphs_table_exist && !cache.Get<PropertyGraphsTable>(key)) {
    std::ostringstream error;
    error << "Property Graph Table name \"" << table_name << "\" is already in use. ";
    error << "Either drop the used name(s), or give other name options in the Property Graph function.\n";
    throw BinderException(error.str());
  }
  return cache.GetOrCreate<PropertyGraphsTable>(key, table_name);
}

void PropertyGraphsTable::InitializeTable(ClientContext &context, const string &catalog_name) {
  auto &catalog = Catalog::GetCatalog(context, catalog_name);

  {
    auto info = make_uniq<CreateTableInfo>(catalog_name, DEFAULT_SCHEMA, table_name);
    info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    // 0. Property graph name
    info->columns.AddColumn(ColumnDefinition("property_graph_name", LogicalType::VARCHAR));
    // 1. Table name
    info->columns.AddColumn(ColumnDefinition("table_name", LogicalType::VARCHAR));
    // 2. Label
    info->columns.AddColumn(ColumnDefinition("label", LogicalType::VARCHAR));
    // 3. Is vertex table
    info->columns.AddColumn(ColumnDefinition("is_vertex_table", LogicalType::BOOLEAN));
    // 4. Source table
    info->columns.AddColumn(ColumnDefinition("source_table", LogicalType::VARCHAR));
    // 5. Source primary key
    info->columns.AddColumn(ColumnDefinition("source_pk", LogicalType::LIST(LogicalType::VARCHAR)));
    // 6. Source foreign key
    info->columns.AddColumn(ColumnDefinition("source_fk", LogicalType::LIST(LogicalType::VARCHAR)));
    // 7. Destination table
    info->columns.AddColumn(ColumnDefinition("destination_table", LogicalType::VARCHAR));
    // 8. Destination primary key
    info->columns.AddColumn(ColumnDefinition("destination_pk", LogicalType::LIST(LogicalType::VARCHAR)));
    // 9. Destination foreign key
    info->columns.AddColumn(ColumnDefinition("destination_fk", LogicalType::LIST(LogicalType::VARCHAR)));
    // 10. Discriminator
    info->columns.AddColumn(ColumnDefinition("discriminator", LogicalType::VARCHAR));
    // 11. Sub labels
    info->columns.AddColumn(ColumnDefinition("sub_labels", LogicalType::LIST(LogicalType::VARCHAR)));

    catalog.CreateTable(context, std::move(info));
  }
}

} // namespace duckdb