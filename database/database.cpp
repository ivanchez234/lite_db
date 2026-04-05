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
    
    if (!(ss >> cmd)) return "ERR_EMPTY_QUERY";
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    // Извлекаем имя таблицы (оно второе во ВСЕХ командах: CREATE, SELECT, SCHEMA...)
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

    // 3. SELECT
    if (cmd == "SELECT") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        std::string key;
        ss >> key;
        return storage.select(table_name, id, trim_cmd(key));
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
        return "OK";
    }

    return "ERR_UNKNOWN_COMMAND";
}