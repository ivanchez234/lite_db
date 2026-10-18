#pragma once
#include <string>
#include <shared_mutex>
#include "../storage/storage.h"
#include "wal.h"

class Database {
private:
    Storage storage;

    std::shared_mutex checkpoint_mutex;

    Wal wal;

    bool is_recovering = false;

    bool log_write(const std::string& query);

public:
    explicit Database(const std::string& wal_path = "wal.log");
    ~Database();

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    std::string execute(const std::string& query);
    void load_config(const std::string& filename);
    void recover_from_wal();
};
