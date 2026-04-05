#include "database.h"
#include <sstream>
#include <algorithm>
#include <iostream>

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

std::string Database::execute(const std::string& query) {
    std::stringstream ss(query);
    std::string cmd, table_name;
    
    // 1. Извлекаем команду (CREATE, SELECT, INSERT и т.д.)
    if (!(ss >> cmd)) return "ERR_EMPTY_QUERY";
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    // --- Команда CREATE (не требует ID, только имя таблицы) ---
    // Синтаксис: CREATE users
    if (cmd == "CREATE") {
        ss >> table_name;
        table_name = trim_cmd(table_name);
        if (table_name.empty()) return "ERR_INVALID_TABLE_NAME";
        
        if (storage.create_table(table_name)) {
            return "OK: Table '" + table_name + "' created";
        }
        return "ERR_TABLE_ALREADY_EXISTS";
    }

    // --- Для всех остальных команд вторым аргументом идет TABLE_NAME ---
    if (!(ss >> table_name)) return "ERR_MISSING_TABLE_NAME";
    table_name = trim_cmd(table_name);

    // --- Команда SELECT ---
    // Синтаксис: SELECT users 10 name
    if (cmd == "SELECT") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        
        std::string key;
        ss >> key;
        key = trim_cmd(key); // Если ключа нет, вернет пустую строку (весь JSON)
        
        return storage.select(table_name, id, key);
    } 

    // --- Команды INSERT и UPDATE ---
    // Синтаксис: INSERT users 10 {"name":"Ivan"}
    else if (cmd == "INSERT" || cmd == "UPDATE") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        
        std::string body;
        std::getline(ss >> std::ws, body);
        body = trim_cmd(body);
        
        if (body.empty()) return "ERR_EMPTY_BODY";

        // Если это INSERT, проверяем, нет ли уже такого ключа в этой таблице
        if (cmd == "INSERT" && storage.exists(table_name, id)) {
            return "ERR_ID_EXISTS_IN_TABLE";
        }
        
        storage.insert(table_name, id, body);
        return "OK";
    }

    // --- Команда DELETE ---
    // Синтаксис: DELETE users 10
    else if (cmd == "DELETE") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        
        if (!storage.exists(table_name, id)) return "ERR_NOT_FOUND";
        
        storage.remove(table_name, id);
        return "OK";
    }

    return "ERR_UNKNOWN_COMMAND";
}