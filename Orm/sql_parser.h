#pragma once
#include <string>
#include <vector>
#include <regex>

namespace SQLParser {
    // Главная функция: берет любую строку и пытается перевести SQL в команды LiteDB
    std::string translate(const std::string& query);

    // Вспомогательные функции
    std::string trim(const std::string& str);
    std::vector<std::string> split(const std::string& str, char delim);
}