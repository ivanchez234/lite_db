#pragma once
#include <string>
#include <vector>


namespace SQLParser {
    std::string translate(const std::string& query);

    std::string trim(const std::string& str);

    bool split_list(const std::string& str, char delim, std::vector<std::string>& out);
}
