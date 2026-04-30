#pragma once
#include <variant>
#include <string>
#include <unordered_map>
#include <optional>
#include <memory_resource> // Required for std::pmr::unordered_map

namespace Fluxor
{
    class Record
    {

    public:
        using FieldKey = std::string;
        using FieldValue = std::variant<int, double, std::string>;
        Record() = default;
        Record(const Record &other) = default;
        Record(Record &&other) = default;
        Record &operator=(const Record &other) = default;
        Record &operator=(Record &&other) = default;
        ~Record() = default;

        void set(const FieldKey &key, const FieldValue &value);
        std::optional<FieldValue> get(const FieldKey &key) const;
        const std::pmr::unordered_map<FieldKey, FieldValue> &getFields() const;

    private:
        std::pmr::unordered_map<FieldKey, FieldValue> data;
    };

}