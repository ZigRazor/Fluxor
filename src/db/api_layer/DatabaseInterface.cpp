#include "DatabaseInterface.h"
#include "Logger.hpp"

using namespace Fluxor;

void DatabaseInterface::put(const StorageEngine::TableName &tableName, const Table::RecordKey &recordKey, const Record &record)
{
    LOG_TRACE("Putting record in table '%s' with key '%s'", tableName.c_str(), recordKey.c_str());
    engine.insert(tableName, recordKey, record);
}

std::optional<Record> DatabaseInterface::get(const StorageEngine::TableName &tableName, const Table::RecordKey &recordKey) const
{
    LOG_TRACE("Getting record from table '%s' with key '%s'", tableName.c_str(), recordKey.c_str());
    return engine.get(tableName, recordKey);
}

void DatabaseInterface::erase(const StorageEngine::TableName &tableName, const Table::RecordKey &recordKey)
{
    LOG_TRACE("Erasing record from table '%s' with key '%s'", tableName.c_str(), recordKey.c_str());
    engine.remove(tableName, recordKey);
}

void DatabaseInterface::createTable(const StorageEngine::TableName &tableName)
{
    LOG_TRACE("Creating table '%s'", tableName.c_str());
    engine.createTable(tableName);
}

void DatabaseInterface::dropTable(const StorageEngine::TableName &tableName)
{
    LOG_TRACE("Dropping table '%s'", tableName.c_str());
    engine.dropTable(tableName);
}

const std::pmr::unordered_map<StorageEngine::TableName, Table> DatabaseInterface::snapshot() const
{
    LOG_TRACE("Taking snapshot of database");
    return engine.snapshot();
}

void DatabaseInterface::restore(const std::pmr::unordered_map<StorageEngine::TableName, Table> &snapshot)
{
    LOG_TRACE("Restoring database from snapshot");
    engine.restore(snapshot);
}