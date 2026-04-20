#pragma once
#include <string>
#include "../storage/storage.h"

class Database {
private:
    Storage storage;

    // --- НОВЫЕ ПОЛЯ ДЛЯ WAL ---
    bool is_recovering = false; // Флаг, чтобы база не писала лог во время своего же восстановления
    void append_to_wal(const std::string& query);
    void clear_wal();
public:
    Database(const std::string& filename);
    ~Database();
    std::string execute(const std::string& query);
    void load_config(const std::string& filename);

    // Метод, который мы вызовем при старте сервера
    void recover_from_wal();
};