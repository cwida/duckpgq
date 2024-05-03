#pragma once
#include "duckdb/storage/object_cache.hpp"


namespace duckdb {

class PropertyGraphsTable : public ObjectCacheEntry {
public:
    PropertyGraphsTable(string table_name_)
            : table_name(std::move(table_name_)), schema(DEFAULT_SCHEMA) {
        }
        string table_name;
        const string schema;

        static shared_ptr<PropertyGraphsTable> GetOrCreate(ClientContext &context, const string &table_name, const string &catalog_name);

        void InitializeTable(ClientContext &context, const string &catalog_name);
        TableCatalogEntry &GetPropertyGraphTable(ClientContext &context, const string &catalog_name) const;

        idx_t GetCurrentFileIndex(idx_t query_id);

        static string ObjectType() {
                return "property_graph_table_cache";
        }

        string GetObjectType() override {
                return ObjectType();
        }
};


}
