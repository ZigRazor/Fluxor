#include <gtest/gtest.h>
#include "db/persistence_layer/SnapshotPersistence.h"
#include "db/storage_engine/StorageEngine.h"
#include "db/memory_layer/Table.h"
#include "db/memory_layer/Record.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <cstdint>
#include <cmath>

using namespace Fluxor;
namespace fs = std::filesystem;

// ============================================================
// Fixture — manages a unique temp file per test
// ============================================================
class SnapshotPersistenceTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Use the OS temp directory + a unique filename to avoid test collisions
        tmpFile = (fs::temp_directory_path() /
                   ("fluxor_test_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) +
                    "_" + ::testing::UnitTest::GetInstance()->current_test_info()->name() +
                    ".snap"))
                      .string();
    }

    void TearDown() override
    {
        // Always clean up, even on failure
        std::error_code ec;
        fs::remove(tmpFile, ec);
    }

    // Convenience: build a Record from an initializer list of key-value pairs
    static Record makeRecord(std::initializer_list<std::pair<std::string, Record::FieldValue>> fields)
    {
        Record r;
        for (const auto &[k, v] : fields)
            r.set(k, v);
        return r;
    }

    std::string tmpFile;
    SnapshotPersistence persistence;
};

// ============================================================
// 1. Round-trip: string field
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_StringField)
{
    StorageEngine engine;
    engine.createTable("users");
    engine.insert("users", "u1", makeRecord({{"name", std::string("Alice")}}));

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    auto result = loaded.get("users", "u1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->get("name").value()), "Alice");
}

// ============================================================
// 2. Round-trip: int field
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_IntField)
{
    StorageEngine engine;
    engine.createTable("counters");
    engine.insert("counters", "c1", makeRecord({{"hits", 42}}));

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    auto result = loaded.get("counters", "c1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<int>(result->get("hits").value()), 42);
}

// ============================================================
// 3. Round-trip: double field
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_DoubleField)
{
    StorageEngine engine;
    engine.createTable("metrics");
    engine.insert("metrics", "m1", makeRecord({{"ratio", 3.14159}}));

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    auto result = loaded.get("metrics", "m1");
    ASSERT_TRUE(result.has_value());
    EXPECT_DOUBLE_EQ(std::get<double>(result->get("ratio").value()), 3.14159);
}

// ============================================================
// 4. Round-trip: all three field types in one record
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_AllFieldTypes)
{
    StorageEngine engine;
    engine.createTable("mixed");
    engine.insert("mixed", "r1", makeRecord({{"label", std::string("sensor_A")}, {"count", 7}, {"value", 2.718281828}}));

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    auto result = loaded.get("mixed", "r1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->get("label").value()), "sensor_A");
    EXPECT_EQ(std::get<int>(result->get("count").value()), 7);
    EXPECT_DOUBLE_EQ(std::get<double>(result->get("value").value()), 2.718281828);
}

// ============================================================
// 5. Multiple tables survive round-trip intact
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_MultipleTables)
{
    StorageEngine engine;
    engine.createTable("users");
    engine.createTable("products");

    engine.insert("users", "u1", makeRecord({{"name", std::string("Alice")}}));
    engine.insert("users", "u2", makeRecord({{"name", std::string("Bob")}}));
    engine.insert("products", "p1", makeRecord({{"sku", std::string("X100")}, {"price", 9.99}}));

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    EXPECT_EQ(std::get<std::string>(loaded.get("users", "u1")->get("name").value()), "Alice");
    EXPECT_EQ(std::get<std::string>(loaded.get("users", "u2")->get("name").value()), "Bob");
    EXPECT_EQ(std::get<std::string>(loaded.get("products", "p1")->get("sku").value()), "X100");
    EXPECT_DOUBLE_EQ(std::get<double>(loaded.get("products", "p1")->get("price").value()), 9.99);
}

// ============================================================
// 6. Multiple records per table
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_MultipleRecordsPerTable)
{
    StorageEngine engine;
    engine.createTable("scores");

    for (int i = 0; i < 100; ++i)
    {
        engine.insert("scores", "r" + std::to_string(i),
                      makeRecord({{"score", i * 10}}));
    }

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    for (int i = 0; i < 100; ++i)
    {
        auto result = loaded.get("scores", "r" + std::to_string(i));
        ASSERT_TRUE(result.has_value()) << "Missing record r" << i;
        EXPECT_EQ(std::get<int>(result->get("score").value()), i * 10);
    }
}

// ============================================================
// 7. Empty database (zero tables)
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_EmptyDatabase)
{
    StorageEngine engine; // no tables

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    EXPECT_NO_THROW(persistence.load(loaded, tmpFile));

    // Nothing to query — just verify the file was valid and load succeeded
    EXPECT_FALSE(loaded.get("nonexistent", "key").has_value());
}

// ============================================================
// 8. Table with zero records
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_EmptyTable)
{
    StorageEngine engine;
    engine.createTable("empty_table");

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    // Table should exist but hold no records
    EXPECT_FALSE(loaded.get("empty_table", "anything").has_value());
}

// ============================================================
// 9. Record with zero fields
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_EmptyRecord)
{
    StorageEngine engine;
    engine.createTable("t");
    engine.insert("t", "empty_record", Record{});

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    auto result = loaded.get("t", "empty_record");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->get("anything").has_value());
}

// ============================================================
// 10. Saving over an existing file overwrites it cleanly
// ============================================================
TEST_F(SnapshotPersistenceTest, Save_OverwritesExistingFile)
{
    // First save: Alice
    StorageEngine engine1;
    engine1.createTable("users");
    engine1.insert("users", "u1", makeRecord({{"name", std::string("Alice")}}));
    persistence.save(engine1, tmpFile);

    // Second save: Bob only — Alice must disappear
    StorageEngine engine2;
    engine2.createTable("users");
    engine2.insert("users", "u1", makeRecord({{"name", std::string("Bob")}}));
    persistence.save(engine2, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    EXPECT_EQ(std::get<std::string>(loaded.get("users", "u1")->get("name").value()), "Bob");
}

// ============================================================
// 11. Loading into a non-empty engine replaces its state
// ============================================================
TEST_F(SnapshotPersistenceTest, Load_ReplacesEngineState)
{
    // Save a snapshot with Alice
    StorageEngine snapshot_engine;
    snapshot_engine.createTable("users");
    snapshot_engine.insert("users", "u1", makeRecord({{"name", std::string("Alice")}}));
    persistence.save(snapshot_engine, tmpFile);

    // Pre-populate the target engine with something different
    StorageEngine engine;
    engine.createTable("other_table");
    engine.insert("other_table", "x", makeRecord({{"val", std::string("should_disappear")}}));

    persistence.load(engine, tmpFile);

    // After restore: Alice is present, old data is gone
    ASSERT_TRUE(engine.get("users", "u1").has_value());
    EXPECT_EQ(std::get<std::string>(engine.get("users", "u1")->get("name").value()), "Alice");
    EXPECT_FALSE(engine.get("other_table", "x").has_value());
}

// ============================================================
// 12. Integer edge cases: zero, negative, INT_MAX
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_IntEdgeCases)
{
    StorageEngine engine;
    engine.createTable("edges");
    engine.insert("edges", "zero", makeRecord({{"v", 0}}));
    engine.insert("edges", "negative", makeRecord({{"v", -1}}));
    engine.insert("edges", "minval", makeRecord({{"v", std::numeric_limits<int32_t>::min()}}));
    engine.insert("edges", "maxval", makeRecord({{"v", std::numeric_limits<int32_t>::max()}}));

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    EXPECT_EQ(std::get<int>(loaded.get("edges", "zero")->get("v").value()), 0);
    EXPECT_EQ(std::get<int>(loaded.get("edges", "negative")->get("v").value()), -1);
    EXPECT_EQ(std::get<int>(loaded.get("edges", "minval")->get("v").value()), std::numeric_limits<int32_t>::min());
    EXPECT_EQ(std::get<int>(loaded.get("edges", "maxval")->get("v").value()), std::numeric_limits<int32_t>::max());
}

// ============================================================
// 13. Double edge cases: 0.0, negative, infinity, NaN
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_DoubleEdgeCases)
{
    StorageEngine engine;
    engine.createTable("doubles");
    engine.insert("doubles", "zero", makeRecord({{"v", 0.0}}));
    engine.insert("doubles", "negative", makeRecord({{"v", -1.5}}));
    engine.insert("doubles", "inf", makeRecord({{"v", std::numeric_limits<double>::infinity()}}));
    engine.insert("doubles", "nan", makeRecord({{"v", std::numeric_limits<double>::quiet_NaN()}}));

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    EXPECT_DOUBLE_EQ(std::get<double>(loaded.get("doubles", "zero")->get("v").value()), 0.0);
    EXPECT_DOUBLE_EQ(std::get<double>(loaded.get("doubles", "negative")->get("v").value()), -1.5);
    EXPECT_TRUE(std::isinf(std::get<double>(loaded.get("doubles", "inf")->get("v").value())));
    EXPECT_TRUE(std::isnan(std::get<double>(loaded.get("doubles", "nan")->get("v").value())));
}

// ============================================================
// 14. String edge cases: empty string, unicode, binary-like content
// ============================================================
TEST_F(SnapshotPersistenceTest, RoundTrip_StringEdgeCases)
{
    StorageEngine engine;
    engine.createTable("strings");
    engine.insert("strings", "empty", makeRecord({{"v", std::string("")}}));
    engine.insert("strings", "unicode", makeRecord({{"v", std::string("こんにちは 🌍")}}));
    engine.insert("strings", "spaces", makeRecord({{"v", std::string("  leading and trailing  ")}}));
    engine.insert("strings", "newline", makeRecord({{"v", std::string("line1\nline2\ttab")}}));

    persistence.save(engine, tmpFile);

    StorageEngine loaded;
    persistence.load(loaded, tmpFile);

    EXPECT_EQ(std::get<std::string>(loaded.get("strings", "empty")->get("v").value()), "");
    EXPECT_EQ(std::get<std::string>(loaded.get("strings", "unicode")->get("v").value()), "こんにちは 🌍");
    EXPECT_EQ(std::get<std::string>(loaded.get("strings", "spaces")->get("v").value()), "  leading and trailing  ");
    EXPECT_EQ(std::get<std::string>(loaded.get("strings", "newline")->get("v").value()), "line1\nline2\ttab");
}

// ============================================================
// 15. File not found throws
// ============================================================
TEST_F(SnapshotPersistenceTest, Load_ThrowsOnMissingFile)
{
    StorageEngine engine;
    EXPECT_THROW(persistence.load(engine, "/nonexistent/path/fluxor_missing.snap"),
                 std::runtime_error);
}

// ============================================================
// 16. Corrupt magic bytes throws
// ============================================================
TEST_F(SnapshotPersistenceTest, Load_ThrowsOnInvalidMagic)
{
    // Write a valid snapshot first, then corrupt the magic header
    StorageEngine engine;
    engine.createTable("t");
    persistence.save(engine, tmpFile);

    {
        std::fstream f(tmpFile, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.is_open());
        f.seekp(0);
        const char bad_magic[4] = {'X', 'X', 'X', 'X'};
        f.write(bad_magic, 4);
    }

    StorageEngine victim;
    EXPECT_THROW(persistence.load(victim, tmpFile), std::runtime_error);
}

// ============================================================
// 17. Unsupported version byte throws
// ============================================================
TEST_F(SnapshotPersistenceTest, Load_ThrowsOnUnsupportedVersion)
{
    StorageEngine engine;
    engine.createTable("t");
    persistence.save(engine, tmpFile);

    {
        std::fstream f(tmpFile, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.is_open());
        f.seekp(4); // version byte is at offset 4 (after 4 magic bytes)
        const char bad_version = 0xFF;
        f.write(&bad_version, 1);
    }

    StorageEngine victim;
    EXPECT_THROW(persistence.load(victim, tmpFile), std::runtime_error);
}

// ============================================================
// 18. Truncated file throws (and does NOT corrupt the engine)
// ============================================================
TEST_F(SnapshotPersistenceTest, Load_ThrowsOnTruncatedFile_AndDoesNotCorruptEngine)
{
    // Save a valid snapshot
    StorageEngine source;
    source.createTable("users");
    source.insert("users", "u1", makeRecord({{"name", std::string("Alice")}, {"age", 30}}));
    persistence.save(source, tmpFile);

    // Truncate the file to 10 bytes — mid-header
    {
        fs::resize_file(tmpFile, 10);
    }

    // Pre-load engine has its own data
    StorageEngine victim;
    victim.createTable("safe_table");
    victim.insert("safe_table", "k1", makeRecord({{"data", std::string("must_survive")}}));

    EXPECT_THROW(persistence.load(victim, tmpFile), std::runtime_error);

    // Atomic restore guarantee: victim's original state must be intact
    auto result = victim.get("safe_table", "k1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->get("data").value()), "must_survive");
}

// ============================================================
// 19. Unknown field type tag throws
// ============================================================
TEST_F(SnapshotPersistenceTest, Load_ThrowsOnUnknownFieldTypeTag)
{
    // Build a valid snapshot
    StorageEngine engine;
    engine.createTable("t");
    engine.insert("t", "r1", makeRecord({{"x", std::string("hello")}}));
    persistence.save(engine, tmpFile);

    // Locate and corrupt the type tag byte.
    // Format: magic(4) + version(1) + tableCount(4) +
    //         tableNameLen(2) + tableName(1) + recordCount(4) +
    //         recordKeyLen(2) + recordKey(2) + fieldCount(2) +
    //         fieldKeyLen(2) + fieldKey(1) + [TYPE TAG HERE]
    // Offset = 4+1+4 + 2+1 + 4 + 2+2 + 2 + 2+1 = 25
    const std::streamoff type_tag_offset = 4 + 1 + 4 + 2 + 1 + 4 + 2 + 2 + 2 + 2 + 1;
    {
        std::fstream f(tmpFile, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.is_open());
        f.seekp(type_tag_offset);
        const char bad_tag = 0x7F; // not a valid FieldTag
        f.write(&bad_tag, 1);
    }

    StorageEngine victim;
    EXPECT_THROW(persistence.load(victim, tmpFile), std::runtime_error);
}

// ============================================================
// 20. File on disk has exactly the expected size
// ============================================================
TEST_F(SnapshotPersistenceTest, Save_ProducesExpectedFileSize)
{
    // One table "t" (1 byte name), one record key "k" (1 byte),
    // one int field "v" = 0.
    //
    // Expected layout:
    //   magic(4) + version(1) + tableCount(4)         =  9
    //   tableNameLen(2) + "t"(1) + recordCount(4)     =  7
    //   recordKeyLen(2) + "k"(1) + fieldCount(2)      =  5
    //   fieldKeyLen(2) + "v"(1) + typeTag(1) + i32(4) =  8
    //                                          TOTAL  = 29

    StorageEngine engine;
    engine.createTable("t");
    engine.insert("t", "k", makeRecord({{"v", 0}}));
    persistence.save(engine, tmpFile);

    const auto fileSize = static_cast<std::uintmax_t>(fs::file_size(tmpFile));
    EXPECT_EQ(fileSize, 29u);
}
