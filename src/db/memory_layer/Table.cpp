#include "Table.h"

using namespace Fluxor;

void Table::insert(const Table::RecordKey &key, const Record &record)
{
    records[key] = record;
}

std::optional<Record> Table::get(const Table::RecordKey &key) const
{
    auto it = records.find(key);
    if (it == records.end())
    {
        return std::nullopt;
    }
    return it->second;
}

const std::pmr::unordered_map<Table::RecordKey, Record> &Table::getRecords() const
{
    return records;
}

void Table::remove(const Table::RecordKey &key)
{
    records.erase(key);
}