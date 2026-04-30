#include <gtest/gtest.h>
#include "db/api_layer/DatabaseInterface.h"
#include "db/storage_engine/StorageEngine.h"
#include "db/memory_layer/Table.h"
#include "db/memory_layer/Record.h"
#include "logger/Logger.hpp"

using namespace Fluxor;

// ------------------------
// Record Tests
// ------------------------
TEST(RecordTest, SetAndGetField)
{
    Record r;
    r.set("name", std::string("Alice"));

    auto value = r.get("name");

    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(std::get<std::string>(*value), "Alice");
}

TEST(RecordTest, MissingFieldReturnsNullopt)
{
    Record r;
    auto value = r.get("missing");

    EXPECT_FALSE(value.has_value());
}

// ------------------------
// Table Tests
// ------------------------
TEST(TableTest, InsertAndRetrieveRecord)
{
    Table t;
    Record r;
    r.set("id", std::string("1"));

    t.insert("1", r);

    auto result = t.get("1");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->get("id").value()), "1");
}

TEST(TableTest, RemoveRecord)
{
    Table t;
    Record r;
    r.set("id", std::string("1"));

    t.insert("1", r);
    t.remove("1");

    auto result = t.get("1");

    EXPECT_FALSE(result.has_value());
}

// ------------------------
// StorageEngine Tests
// ------------------------
TEST(StorageEngineTest, CreateAndInsertAndGet)
{
    StorageEngine engine;

    engine.createTable("users");

    Record r;
    r.set("name", std::string("Bob"));

    engine.insert("users", "1", r);

    auto result = engine.get("users", "1");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->get("name").value()), "Bob");
}

TEST(StorageEngineTest, DropTableRemovesData)
{
    StorageEngine engine;

    engine.createTable("users");

    Record r;
    r.set("name", std::string("Bob"));

    engine.insert("users", "1", r);
    engine.dropTable("users");

    auto result = engine.get("users", "1");

    EXPECT_FALSE(result.has_value());
}

// ------------------------
// DatabaseInterface Tests
// ------------------------
TEST(DatabaseInterfaceTest, PutAndGet)
{
    Logger::instance().set_level(LoggerLevel::Trace);
    DatabaseInterface db;

    db.createTable("users");

    Record r;
    r.set("name", std::string("Alice"));

    db.put("users", "1", r);

    auto result = db.get("users", "1");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->get("name").value()), "Alice");
}

TEST(DatabaseInterfaceTest, EraseRecord)
{
    Logger::instance().set_level(LoggerLevel::Trace);
    DatabaseInterface db;

    db.createTable("users");

    Record r;
    r.set("name", std::string("Alice"));

    db.put("users", "1", r);
    db.erase("users", "1");

    auto result = db.get("users", "1");

    EXPECT_FALSE(result.has_value());
}

// ------------------------
// Snapshot / Restore Tests
// ------------------------
TEST(DatabaseInterfaceTest, SnapshotAndRestore)
{
    Logger::instance().set_level(LoggerLevel::Trace);
    DatabaseInterface db;

    db.createTable("users");

    Record r;
    r.set("name", std::string("Alice"));

    db.put("users", "1", r);

    auto snap = db.snapshot();

    DatabaseInterface db2;
    db2.restore(snap);

    auto result = db2.get("users", "1");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->get("name").value()), "Alice");
}

// ------------------------
// Integration Test
// ------------------------
TEST(FluxorIntegrationTest, FullWorkflow)
{
    Logger::instance().set_level(LoggerLevel::Trace);
    DatabaseInterface db;

    db.createTable("users");

    Record r1;
    r1.set("name", std::string("Alice"));

    Record r2;
    r2.set("name", std::string("Bob"));

    db.put("users", "1", r1);
    db.put("users", "2", r2);

    EXPECT_TRUE(db.get("users", "1").has_value());
    EXPECT_TRUE(db.get("users", "2").has_value());

    db.erase("users", "1");

    EXPECT_FALSE(db.get("users", "1").has_value());
    EXPECT_TRUE(db.get("users", "2").has_value());
}