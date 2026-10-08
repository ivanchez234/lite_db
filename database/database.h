#pragma once
#include <string>
#include <shared_mutex>
#include "../storage/storage.h"
#include "wal.h"


class Database {
private:
    Storage storage;

    // --- ЗАМОК КОНТРОЛЬНОЙ ТОЧКИ ---
    // Это не замок «на всю базу». Записи (INSERT/UPDATE/DELETE) берут его
    // разделяемо и поэтому идут параллельно — друг от друга их защищают
    // замки таблиц внутри Storage. Чтения его не берут вовсе.
    //
    // Эксклюзивно его берут только FLUSH и DDL (CREATE, SCHEMA). FLUSH
    // нужен момент, когда ни одна запись не находится между «уже в журнале»
    // и «уже в буфере»: только тогда после сброса буферов журнал можно
    // очистить, ничего не потеряв.
    std::shared_mutex checkpoint_mutex;

    // --- ЖУРНАЛ ПРЕДЗАПИСИ ---
    Wal wal;

    // Восстановление идёт до запуска сервера, в одном потоке,
    // поэтому обычного bool достаточно.
    bool is_recovering = false;

    // Пишет команду в журнал. Вызывается из Storage под замком таблицы.
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
