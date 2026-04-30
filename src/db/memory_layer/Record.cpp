#include "Record.h"

using namespace Fluxor;

void Record::set(const Record::FieldKey &key, const Record::FieldValue &value)
{
    data[key] = value;
}

std::optional<Record::FieldValue> Record::get(const Record::FieldKey &key) const
{
    auto it = data.find(key);
    if (it == data.end())
    {
        return std::nullopt;
    }
    return it->second;
}

const std::pmr::unordered_map<Record::FieldKey, Record::FieldValue> &Record::getFields() const
{
    return data;
}
