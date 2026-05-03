#pragma once
#include <string>
#include <mutex> // <--- ПОДКЛЮЧАЕМ ДЛЯ УМНЫХ БЛОКИРОВОК
#include "../storage/storage.h"


class Database {
private:
    Storage storage;
    
    // --- НАШ ЗАМОК ---
    // shared_mutex позволяет множественное чтение, но только одиночную запись
    std::mutex db_mutex; 

    // --- НАШ ФАЙЛ ЛОГОВ ---
    std::ofstream wal_file; // Теперь файл живет вместе с базой
    // --- НОВЫЕ ПОЛЯ ДЛЯ WAL ---
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