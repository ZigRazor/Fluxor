#pragma once
#include "Record.h"
#include "Table.h"

namespace Fluxor
{
    class StorageEngine
    {

    public:
        using TableName = std::string;

        StorageEngine() = default;
        StorageEngine(const StorageEngine &other) = default;
        StorageEngine(StorageEngine &&other) = default;
        StorageEngine &operator=(const StorageEngine &other) = default;
        StorageEngine &operator=(StorageEngine &&other) = default;
        ~StorageEngine() = default;

        void createTable(const TableName &name);
        Table &getTable(const TableName &name);
        const Table &getTable(const TableName &name) const;
        void dropTable(const TableName &name);
        void insert(const TableName &name, const Record &record);
        std::optional<Record> get(const TableName &name, const Table::RecordKey &key) const;
        void remove(const TableName &name, const Table::RecordKey &key);
        const std::pmr::unordered_map<TableName, Table> snapshot() const;
        void restore(const std::pmr::unordered_map<TableName, Table> &snapshot);

    private:
        std::pmr::unordered_map<TableName, Table> tables;
    };
}