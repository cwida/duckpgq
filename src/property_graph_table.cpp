#include "property_graph_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"


namespace duckdb {

TableCatalogEntry &PropertyGraphsTable::GetPropertyGraphTable(ClientContext &context) {
  auto &catalog = Catalog::GetCatalog(context, SYSTEM_CATALOG);
  auto &table_entry = catalog.GetEntry<TableCatalogEntry>(context, schema, table_name);
  return table_entry;
}

shared_ptr<PropertyGraphsTable> PropertyGraphsTable::GetOrCreate(ClientContext &context, const string &table_name) {
  auto key = "PROPERTY_GRAPH_TABLE_CACHE_ENTRY_" + StringUtil::Upper(table_name);
  auto &cache = ObjectCache::GetObjectCache(context);
  auto &catalog = Catalog::GetCatalog(context, SYSTEM_CATALOG);

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
} // namespace duckdb