#pragma once
#include <string>
#include <mutex>
#include <shared_mutex>
#include "../storage/storage.h"
#include "wal.h"


class Database {
private:
    Storage storage;
    
    // Разделяемый замок: чтения идут параллельно, запись — эксклюзивно.
    std::shared_mutex db_mutex;

    // --- ЖУРНАЛ ПРЕДЗАПИСИ ---
    Wal wal;
    bool is_recovering = false;

    void append_to_wal(const std::string& query);
    void clear_wal();
public:
    Database(const std::string& filename);
    ~Database();
    std::string execute(const std::string& query);
    void load_config(const std::string& filename);
    void recover_from_wal();
};