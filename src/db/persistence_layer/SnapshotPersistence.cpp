#include "SnapshotPersistence.h"
#include <fstream>
#include <stdexcept>

using namespace Fluxor;

void SnapshotPersistence::save(const StorageEngine &engine, const std::string &filename)
{
    // Implement serialization logic to save the state of the StorageEngine to a file
    // This is a placeholder implementation and should be replaced with actual serialization code
    std::ofstream outFile(filename, std::ios::binary);
    if (!outFile)
    {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    // Serialize the StorageEngine state here

    std::pmr::unordered_map<StorageEngine::TableName, Table> snapshot = engine.snapshot();
    for (const auto &[tableName, table] : snapshot)
    {
        outFile << "Table: " << tableName << std::endl;
        // Serialize each record in the table
        // This is a placeholder and should be replaced with actual serialization logic
        for (const auto &[recordKey, record] : table.getRecords())
        {
            outFile << "Record Key: " << recordKey << std::endl;
            // Serialize each field in the record
            for (const auto &[fieldKey, fieldValue] : record.getFields())
            {
                outFile << "Field Key: " << fieldKey << ", Field Value: " << std::get<std::string>(fieldValue) << std::endl;
            }
        }
    }
    outFile.close();
}

void SnapshotPersistence::load(StorageEngine &engine, const std::string &filename)
{
    // Implement deserialization logic to restore the state of the StorageEngine from a file
    // This is a placeholder implementation and should be replaced with actual deserialization code
    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile)
    {
        throw std::runtime_error("Failed to open file for reading: " + filename);
    }
    // Deserialize the StorageEngine state here
    while (!inFile.eof())
    {
        std::string line;
        std::getline(inFile, line);

        std::string tableName = "";
        Record record;
        // Parse the line to reconstruct the StorageEngine state
        // This is a placeholder and should be replaced with actual deserialization logic
        if (line.find("Table:") != std::string::npos)
        {
            tableName = line.substr(7);
            engine.createTable(tableName);
        }
        else if (line.find("Record Key:") != std::string::npos)
        {
            std::string recordKey = line.substr(12);
            // Create a new record and add it to the current table
            // This is a placeholder and should be replaced with actual deserialization logic
            record.set("id", recordKey); // Assuming "id" is the primary key field
            engine.insert(tableName, record);
        }
        else if (line.find("Field Key:") != std::string::npos)
        {
            // Parse the field key and value and set it in the current record
            // This is a placeholder and should be replaced with actual deserialization logic
            size_t keyStart = line.find("Field Key:") + 10;
            size_t valueStart = line.find("Field Value:") + 12;
            std::string fieldKey = line.substr(keyStart, valueStart - keyStart - 12);
            std::string fieldValue = line.substr(valueStart);
            // Set the field in the current record
            // This is a placeholder and should be replaced with actual deserialization logic
            record.set(fieldKey, fieldValue);
        }
    }
    inFile.close();
}
