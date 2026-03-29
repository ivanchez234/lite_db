#include "database.h"
#include <sstream>

Database::Database(const std::string& filename) : storage(filename) {}

std::string Database::execute(const std::string& query) {
    std::stringstream ss(query);
    std::string command;
    int key;
    
    if (!(ss >> command >> key)) return "ERR: Invalid command format\n";

    std::string value;
    std::getline(ss >> std::ws, value); // Забираем остаток строки как значение

    if (command == "INSERT") {
        storage.insert(key, value);
        return "OK\n";
    }
    if (command == "UPDATE") {
        if (!storage.exists(key)) return "ERR: Key not found\n";
        storage.insert(key, value);
        return "OK\n";
    }
    if (command == "SELECT") {
        return storage.select(key) + "\n";
    }
    if (command == "DELETE") {
        storage.remove(key);
        return "OK\n";
    }

    return "ERR: Unknown command\n";
}