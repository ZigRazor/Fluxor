#include "db/persistence_layer/SnapshotPersistence.h"
#include "logger/Logger.hpp"

#include <fstream>
#include <stdexcept>
#include <cstring>
#include <cstdint>
#include <array>

// ============================================================
// Binary format specification
// ============================================================
//
//  Offset  Size  Description
//  ------  ----  -----------
//  0       4     Magic bytes: 'F' 'L' 'X' 'R'
//  4       1     Format version (currently 1)
//  5       4     Table count  (uint32_t, LE)
//
//  Per table:
//    0     2     Table name length (uint16_t, LE)
//    2     N     Table name bytes
//    N+2   4     Record count (uint32_t, LE)
//
//    Per record:
//      0   2     Record key length (uint16_t, LE)
//      2   N     Record key bytes
//      N+2 2     Field count (uint16_t, LE)
//
//      Per field:
//        0  2    Field key length (uint16_t, LE)
//        2  N    Field key bytes
//        N+2 1   Type tag:
//                  0x00 = int    → int32_t  (4 bytes, LE)
//                  0x01 = double → IEEE-754 (8 bytes, LE)
//                  0x02 = string → uint32_t length + bytes
//
// ============================================================

using namespace Fluxor;

namespace
{
    // ----------------------------------------------------------
    // Format constants
    // ----------------------------------------------------------
    constexpr std::array<char, 4> MAGIC         = { 'F', 'L', 'X', 'R' };
    constexpr uint8_t             FORMAT_VERSION = 1;

    enum class FieldTag : uint8_t
    {
        Int    = 0x00,
        Double = 0x01,
        String = 0x02,
    };

    // ----------------------------------------------------------
    // Low-level write helpers (little-endian, no heap allocation)
    // ----------------------------------------------------------
    void write_u8(std::ostream &out, uint8_t v)
    {
        out.write(reinterpret_cast<const char *>(&v), sizeof(v));
    }

    void write_u16(std::ostream &out, uint16_t v)
    {
        // Encode as little-endian explicitly for portability
        const uint8_t buf[2] = {
            static_cast<uint8_t>(v & 0xFF),
            static_cast<uint8_t>((v >> 8) & 0xFF)
        };
        out.write(reinterpret_cast<const char *>(buf), 2);
    }

    void write_u32(std::ostream &out, uint32_t v)
    {
        const uint8_t buf[4] = {
            static_cast<uint8_t>(v & 0xFF),
            static_cast<uint8_t>((v >> 8)  & 0xFF),
            static_cast<uint8_t>((v >> 16) & 0xFF),
            static_cast<uint8_t>((v >> 24) & 0xFF)
        };
        out.write(reinterpret_cast<const char *>(buf), 4);
    }

    void write_i32(std::ostream &out, int32_t v)
    {
        // Reinterpret as unsigned bits, then write LE
        uint32_t u{};
        std::memcpy(&u, &v, sizeof(u));
        write_u32(out, u);
    }

    void write_f64(std::ostream &out, double v)
    {
        // IEEE-754 double written as 8 raw bytes (LE)
        uint64_t bits{};
        std::memcpy(&bits, &v, sizeof(bits));
        const uint8_t buf[8] = {
            static_cast<uint8_t>(bits & 0xFF),
            static_cast<uint8_t>((bits >> 8)  & 0xFF),
            static_cast<uint8_t>((bits >> 16) & 0xFF),
            static_cast<uint8_t>((bits >> 24) & 0xFF),
            static_cast<uint8_t>((bits >> 32) & 0xFF),
            static_cast<uint8_t>((bits >> 40) & 0xFF),
            static_cast<uint8_t>((bits >> 48) & 0xFF),
            static_cast<uint8_t>((bits >> 56) & 0xFF)
        };
        out.write(reinterpret_cast<const char *>(buf), 8);
    }

    // Writes a length-prefixed string: uint16_t length + raw bytes
    // Maximum string length: 65535 bytes
    void write_string16(std::ostream &out, const std::string &s)
    {
        if (s.size() > 0xFFFF)
            throw std::runtime_error("SnapshotPersistence: string exceeds 65535 bytes: " + s.substr(0, 64));

        write_u16(out, static_cast<uint16_t>(s.size()));
        out.write(s.data(), static_cast<std::streamsize>(s.size()));
    }

    // Writes a length-prefixed string: uint32_t length + raw bytes (for field string values)
    void write_string32(std::ostream &out, const std::string &s)
    {
        write_u32(out, static_cast<uint32_t>(s.size()));
        out.write(s.data(), static_cast<std::streamsize>(s.size()));
    }

    // ----------------------------------------------------------
    // Low-level read helpers (little-endian)
    // ----------------------------------------------------------
    void expect_read(std::istream &in, void *buf, std::streamsize n, const char *ctx)
    {
        in.read(reinterpret_cast<char *>(buf), n);
        if (in.gcount() != n)
            throw std::runtime_error(std::string("SnapshotPersistence: unexpected EOF while reading ") + ctx);
    }

    uint8_t read_u8(std::istream &in)
    {
        uint8_t v{};
        expect_read(in, &v, 1, "u8");
        return v;
    }

    uint16_t read_u16(std::istream &in)
    {
        uint8_t buf[2]{};
        expect_read(in, buf, 2, "u16");
        return static_cast<uint16_t>(buf[0]) |
               (static_cast<uint16_t>(buf[1]) << 8);
    }

    uint32_t read_u32(std::istream &in)
    {
        uint8_t buf[4]{};
        expect_read(in, buf, 4, "u32");
        return static_cast<uint32_t>(buf[0])        |
               (static_cast<uint32_t>(buf[1]) << 8)  |
               (static_cast<uint32_t>(buf[2]) << 16) |
               (static_cast<uint32_t>(buf[3]) << 24);
    }

    int32_t read_i32(std::istream &in)
    {
        uint32_t u = read_u32(in);
        int32_t v{};
        std::memcpy(&v, &u, sizeof(v));
        return v;
    }

    double read_f64(std::istream &in)
    {
        uint8_t buf[8]{};
        expect_read(in, buf, 8, "f64");
        uint64_t bits =
            static_cast<uint64_t>(buf[0])        |
            (static_cast<uint64_t>(buf[1]) << 8)  |
            (static_cast<uint64_t>(buf[2]) << 16) |
            (static_cast<uint64_t>(buf[3]) << 24) |
            (static_cast<uint64_t>(buf[4]) << 32) |
            (static_cast<uint64_t>(buf[5]) << 40) |
            (static_cast<uint64_t>(buf[6]) << 48) |
            (static_cast<uint64_t>(buf[7]) << 56);
        double v{};
        std::memcpy(&v, &bits, sizeof(v));
        return v;
    }

    std::string read_string16(std::istream &in)
    {
        uint16_t len = read_u16(in);
        std::string s(len, '\0');
        if (len > 0)
            expect_read(in, s.data(), len, "string16 data");
        return s;
    }

    std::string read_string32(std::istream &in)
    {
        uint32_t len = read_u32(in);
        std::string s(len, '\0');
        if (len > 0)
            expect_read(in, s.data(), len, "string32 data");
        return s;
    }

    // ----------------------------------------------------------
    // Record serialization
    // ----------------------------------------------------------
    void serialize_record(std::ostream &out, const Record &record)
    {
        const auto &fields = record.getFields();

        if (fields.size() > 0xFFFF)
            throw std::runtime_error("SnapshotPersistence: record has more than 65535 fields");

        write_u16(out, static_cast<uint16_t>(fields.size()));

        for (const auto &[fieldKey, fieldValue] : fields)
        {
            write_string16(out, fieldKey);

            std::visit([&out](const auto &val)
            {
                using T = std::decay_t<decltype(val)>;

                if constexpr (std::is_same_v<T, int>)
                {
                    write_u8(out, static_cast<uint8_t>(FieldTag::Int));
                    write_i32(out, static_cast<int32_t>(val));
                }
                else if constexpr (std::is_same_v<T, double>)
                {
                    write_u8(out, static_cast<uint8_t>(FieldTag::Double));
                    write_f64(out, val);
                }
                else if constexpr (std::is_same_v<T, std::string>)
                {
                    write_u8(out, static_cast<uint8_t>(FieldTag::String));
                    write_string32(out, val);
                }
            }, fieldValue);
        }
    }

    Record deserialize_record(std::istream &in)
    {
        Record record;
        const uint16_t fieldCount = read_u16(in);

        for (uint16_t i = 0; i < fieldCount; ++i)
        {
            std::string fieldKey = read_string16(in);
            const uint8_t rawTag = read_u8(in);

            switch (static_cast<FieldTag>(rawTag))
            {
                case FieldTag::Int:
                    record.set(fieldKey, static_cast<int>(read_i32(in)));
                    break;

                case FieldTag::Double:
                    record.set(fieldKey, read_f64(in));
                    break;

                case FieldTag::String:
                    record.set(fieldKey, read_string32(in));
                    break;

                default:
                    throw std::runtime_error(
                        "SnapshotPersistence: unknown field type tag: " + std::to_string(rawTag));
            }
        }

        return record;
    }

} // anonymous namespace

// ============================================================
// Public API
// ============================================================

void SnapshotPersistence::save(const StorageEngine &engine, const std::string &filename)
{
    LOG_INFO("Saving snapshot to '%s'", filename.c_str());

    std::ofstream out(filename, std::ios::binary | std::ios::trunc);
    if (!out)
        throw std::runtime_error("SnapshotPersistence: cannot open file for writing: " + filename);

    // --- Header ---
    out.write(MAGIC.data(), MAGIC.size());
    write_u8(out, FORMAT_VERSION);

    const auto &tables = engine.snapshot();

    if (tables.size() > 0xFFFFFFFF)
        throw std::runtime_error("SnapshotPersistence: too many tables");

    write_u32(out, static_cast<uint32_t>(tables.size()));

    // --- Tables ---
    for (const auto &[tableName, table] : tables)
    {
        write_string16(out, tableName);

        const auto &records = table.getRecords();
        write_u32(out, static_cast<uint32_t>(records.size()));

        LOG_DEBUG("  Serializing table '%s' (%zu records)", tableName.c_str(), records.size());

        for (const auto &[recordKey, record] : records)
        {
            write_string16(out, recordKey);
            serialize_record(out, record);
        }
    }

    if (!out)
        throw std::runtime_error("SnapshotPersistence: write error while saving: " + filename);

    LOG_INFO("Snapshot saved successfully (%zu tables)", tables.size());
}

void SnapshotPersistence::load(StorageEngine &engine, const std::string &filename)
{
    LOG_INFO("Loading snapshot from '%s'", filename.c_str());

    std::ifstream in(filename, std::ios::binary);
    if (!in)
        throw std::runtime_error("SnapshotPersistence: cannot open file for reading: " + filename);

    // --- Validate header ---
    std::array<char, 4> magic{};
    expect_read(in, magic.data(), 4, "magic");
    if (magic != MAGIC)
        throw std::runtime_error("SnapshotPersistence: invalid magic bytes — not a Fluxor snapshot");

    const uint8_t version = read_u8(in);
    if (version != FORMAT_VERSION)
        throw std::runtime_error(
            "SnapshotPersistence: unsupported format version " + std::to_string(version) +
            " (expected " + std::to_string(FORMAT_VERSION) + ")");

    // --- Restore into a fresh snapshot, then swap atomically ---
    std::pmr::unordered_map<StorageEngine::TableName, Table> snapshot;

    const uint32_t tableCount = read_u32(in);
    LOG_DEBUG("Loading %u table(s) from snapshot", tableCount);

    for (uint32_t t = 0; t < tableCount; ++t)
    {
        std::string tableName = read_string16(in);
        const uint32_t recordCount = read_u32(in);

        LOG_DEBUG("  Loading table '%s' (%u records)", tableName.c_str(), recordCount);

        Table table;
        for (uint32_t r = 0; r < recordCount; ++r)
        {
            std::string recordKey = read_string16(in);
            table.insert(recordKey, deserialize_record(in));
        }

        snapshot.emplace(std::move(tableName), std::move(table));
    }

    // Atomic commit: swap only when the entire file has been parsed without error
    engine.restore(snapshot);

    LOG_INFO("Snapshot loaded successfully (%u tables)", tableCount);
}
