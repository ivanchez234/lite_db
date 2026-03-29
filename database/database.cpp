#include "database.h"
#include <sstream>
#include <algorithm>
#include <iostream>

// Вспомогательная функция для обрезки пробелов
std::string trim(const std::string& s) {
    size_t first = s.find_first_not_of(" \n\r\t");
    if (first == std::string::npos) return "";
    size_t last = s.find_last_not_of(" \n\r\t");
    return s.substr(first, (last - first + 1));
}

Database::Database(const std::string& filename) {
    // В новой версии Storage сам управляет сегментами, 
    // поэтому filename можно просто проигнорировать.
}

std::string Database::execute(const std::string& query) {
    std::string trimmed_query = trim(query);
    if (trimmed_query.empty()) return "ERR_EMPTY_QUERY";

    std::stringstream ss(trimmed_query);
    std::string command;
    
    // 1. Читаем команду
    if (!(ss >> command)) return "ERR_INVALID_FORMAT";
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    // --- ЛОГИКА INSERT ---
    if (command == "INSERT") {
        int key;
        if (!(ss >> key)) return "ERR_INVALID_KEY";

        // Читаем остаток строки как потенциальный JSON
        std::string value;
        std::getline(ss >> std::ws, value);
        
        // Убираем точку с запятой в конце, если она пришла от сервера
        if (!value.empty() && value.back() == ';') value.pop_back();
        value = trim(value);

        // КРИТИЧЕСКАЯ ПРОВЕРКА 1: Защита от "13 13 {json}"
        // Если данные начинаются не с '{', значит между ключом и JSON есть мусор
        if (value.empty() || value.front() != '{') {
            return "ERR_EXTRA_DATA_BEFORE_JSON";
        }

        // КРИТИЧЕСКАЯ ПРОВЕРКА 2: Базовая валидация JSON
        if (value.back() != '}') {
            return "ERR_INVALID_JSON_FORMAT (MUST END WITH '}')";
        }

        // КРИТИЧЕСКАЯ ПРОВЕРКА 3: Дубликаты
        if (storage.exists(key)) {
            return "ERR_KEY_ALREADY_EXISTS (USE UPDATE OR DELETE FIRST)";
        }

        storage.insert(key, value);
        return "OK";
    }

    // --- UPDATE (Только для существующих) ---
    else if (command == "UPDATE") {
        int key;
        if (!(ss >> key)) return "ERR_INVALID_KEY";
        
        // Проверка: если ключа нет, обновлять нечего
        if (!storage.exists(key)) return "ERR_KEY_NOT_FOUND";

        std::string value;
        std::getline(ss >> std::ws, value);
        if (!value.empty() && value.back() == ';') value.pop_back();
        
        if (trim(value).front() != '{') return "ERR_INVALID_JSON";

        // В Storage метод insert просто перезапишет смещение в хеш-карте
        storage.insert(key, value); 
        return "OK";
    }
    
    // --- ЛОГИКА SELECT ---
    else if (command == "SELECT") {
        int key;
        if (!(ss >> key)) return "ERR_INVALID_KEY";
        
        std::string result = storage.select(key);
        return (result == "NULL") ? "ERR_NOT_FOUND" : result;
    }

    // --- ЛОГИКА DELETE ---
    else if (command == "DELETE") {
        int key;
        if (!(ss >> key)) return "ERR_INVALID_KEY";
        
        if (!storage.exists(key)) return "ERR_NOT_FOUND";
        storage.remove(key);
        return "OK";
    }

    // --- ЛОГИКА EXISTS ---
    else if (command == "EXISTS") {
        int key;
        if (!(ss >> key)) return "ERR_INVALID_KEY";
        return storage.exists(key) ? "TRUE" : "FALSE";
    }

    return "ERR_UNKNOWN_COMMAND";
}