#include "database.h"
#include <sstream>
#include <algorithm>
#include <iostream>
#include <fstream>

// Вспомогательная функция для очистки строк от мусора
std::string trim_cmd(const std::string& s) {
    size_t first = s.find_first_not_of(" \n\r\t;");
    if (first == std::string::npos) return "";
    size_t last = s.find_last_not_of(" \n\r\t;");
    return s.substr(first, (last - first + 1));
}

Database::Database(const std::string& dummy) {
    // В новой архитектуре Storage сам управляет папкой data/
    // Параметр конструктора можно оставить для совместимости
}
Database::~Database() {
    std::cout << "[System] Flushing buffers to disk before shutdown..." << std::endl;
    for (auto& pair : storage.get_all_tables()) { 
        storage.flush_block_to_disk(pair.second); 
    }
    clear_wal(); // Данные надежно на диске, черновик можно сжечь!
}

// --- РЕАЛИЗАЦИЯ WAL ---

void Database::append_to_wal(const std::string& query) {
    if (is_recovering) return; // Во время восстановления лог не пишем
    
    // Открываем файл в режиме добавления (app)
    std::ofstream wal("wal.log", std::ios::app);
    if (wal.is_open()) {
        wal << query << "\n";
        wal.flush(); // ГАРАНТИРУЕМ, что ОС сбросила текст на диск
    }
}

void Database::clear_wal() {
    // Открытие с флагом trunc мгновенно стирает содержимое файла
    std::ofstream wal("wal.log", std::ios::trunc); 
}

void Database::recover_from_wal() {
    std::ifstream wal("wal.log");
    if (!wal.is_open()) return; // Лога нет, значит всё закрылось штатно

    // Проверяем, не пустой ли файл
    wal.seekg(0, std::ios::end);
    if (wal.tellg() == 0) return;
    wal.seekg(0, std::ios::beg);

    std::cout << "\n[WAL] Crash detected! Found uncommitted operations." << std::endl;
    std::cout << "[WAL] Replaying log to restore Write Buffer..." << std::endl;
    
    is_recovering = true; // Включаем режим восстановления
    std::string line;
    int count = 0;
    
    while (std::getline(wal, line)) {
        if (line.empty()) continue;
        execute(line); // "Скармливаем" команду базе, будто её прислал клиент!
        count++;
    }
    
    is_recovering = false; // Выключаем режим
    std::cout << "[WAL] Successfully recovered " << count << " operations!\n" << std::endl;
}

void Database::load_config(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cout << "[Config] No setup.yaml found, skipping auto-init." << std::endl;
        return;
    }

    std::string line, current_table;
    std::vector<Column> current_cols;
    bool in_schema = false; // Флаг: находимся ли мы внутри блока schema:

    while (std::getline(file, line)) {
        // Пропускаем совсем пустые строки
        if (line.find_first_not_of(" \t\n\r") == std::string::npos) continue;

        // 1. Проверяем отступ (количество пробелов в начале)
        size_t indent = line.find_first_not_of(' ');

        // 2. Если нашли "- name:" с МАЛЕНЬКИМ отступом (обычно 2) — это новая ТАБЛИЦА
        if (line.find("- name:") != std::string::npos && indent < 4) {
            // Сохраняем предыдущую таблицу перед переключением
            if (!current_table.empty() && !current_cols.empty()) {
                storage.create_table(current_table);
                storage.set_schema(current_table, current_cols);
                std::cout << "[Config] Table '" << current_table << "' initialized from YAML." << std::endl;
            }
            
            current_table = trim_cmd(line.substr(line.find(":") + 1));
            current_cols.clear();
            in_schema = false; // Сбрасываем флаг схемы, так как началась новая таблица
        } 
        // 3. Если встретили ключевое слово "schema:" — переходим в режим чтения колонок
        else if (line.find("schema:") != std::string::npos) {
            in_schema = true;
        }
        // 4. Если мы в режиме схемы и видим строку, начинающуюся с "- " — это КОЛОНКА
        else if (in_schema && line.find("- ") != std::string::npos) {
            size_t colon = line.find(":");
            if (colon != std::string::npos) {
                // Извлекаем имя колонки (между "- " и ":")
                size_t dash_pos = line.find("- ");
                std::string col_name = trim_cmd(line.substr(dash_pos + 2, colon - (dash_pos + 2)));
                std::string type_str = trim_cmd(line.substr(colon + 1));

                DataType type = DataType::STRING;
                if (type_str == "INT") type = DataType::INT;
                else if (type_str == "DOUBLE") type = DataType::DOUBLE;
                else if (type_str == "BOOL") type = DataType::BOOL;
                else if (type_str == "DATE") type = DataType::DATE;

                current_cols.push_back({col_name, type});
            }
        }
    }

    // Сохраняем самую последнюю таблицу из файла (она всегда выводится здесь)
    if (!current_table.empty() && !current_cols.empty()) {
        storage.create_table(current_table);
        storage.set_schema(current_table, current_cols);
        std::cout << "[Config] Table '" << current_table << "' initialized from YAML." << std::endl;
    }
}
std::string Database::execute(const std::string& query) {
    std::stringstream ss(query);
    std::string cmd, table_name;
    
    if (!(ss >> cmd)) return "ERR_EMPTY_QUERY";
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    // 0. FLUSH - принудительный сброс буферов
    if (cmd == "FLUSH") {
        for (auto& pair : storage.get_all_tables()) {
            storage.flush_block_to_disk(pair.second);
        }
        clear_wal(); // <--- ОБЯЗАТЕЛЬНО: Очищаем WAL после успешного сброса
        return "OK: All buffers flushed to disk";
    }

    

    // Извлекаем имя таблицы...
    if (!(ss >> table_name)) return "ERR_MISSING_TABLE_NAME";
    table_name = trim_cmd(table_name);

    // 1. CREATE
    if (cmd == "CREATE") {
        if (storage.create_table(table_name)) {
            return "OK: Table '" + table_name + "' created";
        }
        return "ERR_TABLE_ALREADY_EXISTS";
    }

    // 2. SCHEMA
    if (cmd == "SCHEMA") {
        std::vector<Column> cols;
        std::string pair;
        while (ss >> pair) {
            size_t colon = pair.find(':');
            if (colon == std::string::npos) continue;
            
            std::string col_name = pair.substr(0, colon);
            std::string type_str = trim_cmd(pair.substr(colon + 1));
            
            DataType type = DataType::STRING;
            if (type_str == "INT") type = DataType::INT;
            else if (type_str == "DOUBLE") type = DataType::DOUBLE;
            else if (type_str == "BOOL") type = DataType::BOOL;
            
            cols.push_back({col_name, type});
        }
        
        if (cols.empty()) return "ERR_EMPTY_SCHEMA";
        if (storage.set_schema(table_name, cols)) return "OK: Schema applied";
        return "ERR_TABLE_NOT_FOUND";
    }

        // В файле database.cpp внутри метода execute
    if (cmd == "SELECT") {
        std::string arg;
        if (!(ss >> arg)) return "ERR_MISSING_ARGUMENTS";
        
        // Приводим к верхнему регистру для сравнения с "ALL"
        std::string upper_arg = arg;
        std::transform(upper_arg.begin(), upper_arg.end(), upper_arg.begin(), ::toupper);

        // Вариант 1: Выборка всех данных
        if (upper_arg == "ALL" || upper_arg == "*") {
            return storage.select_all(table_name);
        }

        // Вариант 2: Выборка по конкретному ID
        try {
            int id = std::stoi(arg); // Пробуем конвертировать строку в число
            std::string key;
            if (ss >> key) {
                key = trim_cmd(key);
            }
            return storage.select(table_name, id, key);
        } catch (...) {
            return "ERR_INVALID_ID_OR_COMMAND (Expected INT or ALL)";
        }
    }


    // 4. INSERT / UPDATE
    if (cmd == "INSERT" || cmd == "UPDATE") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        std::string body;
        std::getline(ss >> std::ws, body);
        body = trim_cmd(body);
        
        if (body.empty()) return "ERR_EMPTY_BODY";
        if (cmd == "INSERT" && storage.exists(table_name, id)) {
            return "ERR_ID_EXISTS";
        }
        
        try {
            storage.insert(table_name, id, body);
            append_to_wal(query); // <--- ПИШЕМ ЛОГ ТОЛЬКО ПОСЛЕ УСПЕШНОЙ ВСТАВКИ
            return "OK";
        } catch (const std::exception& e) {
            return std::string("ERR: ") + e.what();
        }
    }

    // 5. DELETE
    if (cmd == "DELETE") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        if (!storage.exists(table_name, id)) return "ERR_NOT_FOUND";
        storage.remove(table_name, id);
        append_to_wal(query); // <--- ПИШЕМ ЛОГ ПОСЛЕ УСПЕШНОГО УДАЛЕНИЯ
        return "OK";
    }

    return "ERR_UNKNOWN_COMMAND";
}