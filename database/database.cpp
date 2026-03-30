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
    std::stringstream ss(query);
    std::string cmd;
    ss >> cmd;
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    if (cmd == "SELECT") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        std::string key;
        ss >> key;
        if (!key.empty() && key.back() == ';') key.pop_back();
        return storage.select(id, key);
    } 
    else if (cmd == "INSERT" || cmd == "UPDATE") {
        int id;
        ss >> id;
        std::string body;
        std::getline(ss >> std::ws, body);
        if (!body.empty() && body.back() == ';') body.pop_back();
        
        if (cmd == "INSERT" && storage.exists(id)) return "ERR_EXISTS";
        storage.insert(id, body);
        return "OK";
    }
    else if (cmd == "DELETE") {
        int id;
        ss >> id;
        storage.remove(id);
        return "OK";
    }
    return "ERR_UNKNOWN_COMMAND";
}