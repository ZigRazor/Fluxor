#pragma once
#include "db/storage_engine/StorageEngine.h"

namespace Fluxor
{
    class DatabaseInterface
    {
    public:
        DatabaseInterface() = default;
        DatabaseInterface(const DatabaseInterface &other) = default;
        DatabaseInterface(DatabaseInterface &&other) = default;
        DatabaseInterface &operator=(const DatabaseInterface &other) = default;
        DatabaseInterface &operator=(DatabaseInterface &&other) = default;
        ~DatabaseInterface() = default;

        void put(const StorageEngine::TableName &tableName, const Table::RecordKey &recordKey, const Record &record);
        std::optional<Record> get(const StorageEngine::TableName &tableName, const Table::RecordKey &recordKey) const;
        void erase(const StorageEngine::TableName &tableName, const Table::RecordKey &recordKey);
        void createTable(const StorageEngine::TableName &tableName);
        void dropTable(const StorageEngine::TableName &tableName);
        const std::pmr::unordered_map<StorageEngine::TableName, Table> snapshot() const;
        void restore(const std::pmr::unordered_map<StorageEngine::TableName, Table> &snapshot);

    private:
        StorageEngine engine;
    };
} // namespace Fluxor