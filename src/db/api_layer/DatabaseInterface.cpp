#include "DatabaseInterface.h"

using namespace Fluxor;

void DatabaseInterface::put(const StorageEngine::TableName &tableName, const Record &record)
{
    engine.insert(tableName, record);
}

std::optional<Record> DatabaseInterface::get(const StorageEngine::TableName &tableName, const Table::RecordKey &recordKey) const
{
    return engine.get(tableName, recordKey);
}

void DatabaseInterface::erase(const StorageEngine::TableName &tableName, const Table::RecordKey &recordKey)
{
    engine.remove(tableName, recordKey);
}

void DatabaseInterface::createTable(const StorageEngine::TableName &tableName)
{
    engine.createTable(tableName);
}

void DatabaseInterface::dropTable(const StorageEngine::TableName &tableName)
{
    engine.dropTable(tableName);
}

const std::pmr::unordered_map<StorageEngine::TableName, Table> DatabaseInterface::snapshot() const
{
    return engine.snapshot();
}

void DatabaseInterface::restore(const std::pmr::unordered_map<StorageEngine::TableName, Table> &snapshot)
{
    engine.restore(snapshot);
}