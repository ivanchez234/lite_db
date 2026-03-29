#pragma once
#include <string>
#include "../storage/storage.h"

class Database {
private:
    Storage storage;
public:
    Database(const std::string& filename);
    std::string execute(const std::string& query);
};