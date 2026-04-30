#pragma once

#include "Record.h"

namespace Fluxor
{
    class Table
    {
    public:
        using RecordKey = std::string;
        Table() = default;
        Table(const Table &other) = default;
        Table(Table &&other) = default;
        Table &operator=(const Table &other) = default;
        Table &operator=(Table &&other) = default;
        ~Table() = default;

        void insert(const RecordKey &key, const Record &record);
        std::optional<Record> get(const RecordKey &key) const;
        const std::pmr::unordered_map<RecordKey, Record> &getRecords() const;
        void remove(const RecordKey &key);

    private:
        std::pmr::unordered_map<RecordKey, Record> records;
    };
}