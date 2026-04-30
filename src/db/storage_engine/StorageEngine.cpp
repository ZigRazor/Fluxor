#include "db/storage_engine/StorageEngine.h"

using namespace Fluxor;

void StorageEngine::createTable(const TableName &name)
{
    tables[name] = Table();
}

Table &StorageEngine::getTable(const TableName &name)
{
    return tables[name];
}

const Table &StorageEngine::getTable(const TableName &name) const
{
    return tables.at(name);
}

void StorageEngine::dropTable(const TableName &name)
{
    tables.erase(name);
}

void StorageEngine::insert(const TableName &name, const Table::RecordKey &key, const Record &record)
{
    tables[name].insert(key, record);
}

std::optional<Record> StorageEngine::get(const TableName &name, const Table::RecordKey &key) const
{
    auto tableIt = tables.find(name);
    if (tableIt == tables.end())
    {
        return std::nullopt;
    }
    return tableIt->second.get(key);
}

void StorageEngine::remove(const TableName &name, const Table::RecordKey &key)
{
    tables[name].remove(key);
}

const std::pmr::unordered_map<StorageEngine::TableName, Table> StorageEngine::snapshot() const
{
    return tables;
}

void StorageEngine::restore(const std::pmr::unordered_map<TableName, Table> &snapshot)
{
    tables = snapshot;
}