#pragma once

#include "StorageEngine.h"

namespace Fluxor
{
    class SnapshotPersistence
    {
    public:
        SnapshotPersistence() = default;
        SnapshotPersistence(const SnapshotPersistence &other) = default;
        SnapshotPersistence(SnapshotPersistence &&other) = default;
        SnapshotPersistence &operator=(const SnapshotPersistence &other) = default;
        SnapshotPersistence &operator=(SnapshotPersistence &&other) = default;
        ~SnapshotPersistence() = default;

        void save(const StorageEngine &engine, const std::string &filename);
        void load(StorageEngine &engine, const std::string &filename);
    };
}