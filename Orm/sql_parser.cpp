#include "sql_parser.h"
#include <sstream>
#include <iostream>
#include <algorithm>

namespace SQLParser {

    std::string trim(const std::string& str) {
        size_t first = str.find_first_not_of(" \t\n\r");
        if (std::string::npos == first) return "";
        size_t last = str.find_last_not_of(" \t\n\r");
        return str.substr(first, (last - first + 1));
    }

    std::vector<std::string> split(const std::string& str, char delim) {
        std::vector<std::string> tokens;
        std::stringstream ss(str);
        std::string token;
        while (std::getline(ss, token, delim)) {
            tokens.push_back(trim(token));
        }
        return tokens;
    }

    std::string translate(const std::string& raw_query) {
        // Очищаем запрос от невидимых символов переноса строки (важно для сокетов!)
        std::string query = trim(raw_query); 
        if (query.empty()) return "";

        std::string upper_query = query;
        std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);

        // Бронебойная защита: если что-то пойдет не так, сервер не упадет
        try { 
            // 1. ПАРСИМ INSERT
            if (upper_query.find("INSERT INTO") == 0) {
                std::regex re(R"(INSERT\s+INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\))", std::regex::icase);
                std::smatch match;
                if (std::regex_search(query, match, re)) {
                    std::string table = match[1].str();
                    auto cols = split(match[2].str(), ',');
                    auto vals = split(match[3].str(), ',');

                    // Защита от несовпадения количества колонок и значений
                    if (cols.size() != vals.size()) return "ERR_SQL_PARSER: Columns and values mismatch";

                    std::string id = "-1";
                    std::string json = "{";
                    
                    for (size_t i = 0; i < cols.size(); ++i) {
                        std::string c = cols[i];
                        std::string v = vals[i];
                        
                        // Защита от пустых значений
                        if (!v.empty() && v.front() == '\'' && v.back() == '\'') {
                            v = "\"" + v.substr(1, v.size() - 2) + "\""; 
                        }
                        
                        if (c == "id") id = v;
                        json += "\"" + c + "\":" + v;
                        if (i < cols.size() - 1) json += ",";
                    }
                    json += "}";
                    return "INSERT " + table + " " + id + " " + json;
                }
            }

            // 2. ПАРСИМ SELECT
            // Обновленный блок для SELECT в sql_parser.cpp
            // Кусок в sql_parser.cpp
            if (upper_query.find("SELECT") == 0) {
                // Эта регулярка игнорирует всё лишнее между SELECT и FROM
                std::regex re_with_id(R"(SELECT\s+.*?\s+FROM\s+(\w+).*?WHERE\s+.*?\bid\s*=\s*(\d+))", std::regex::icase);
                std::regex re_all(R"(SELECT\s+.*?\s+FROM\s+(\w+))", std::regex::icase);
                std::smatch match;

                if (std::regex_search(query, match, re_with_id)) {
                    return "SELECT " + match[1].str() + " " + match[2].str();
                } else if (std::regex_search(query, match, re_all)) {
                    return "SELECT " + match[1].str() + " ALL";
                }
            }

            // 3. ПАРСИМ DELETE
            // 3. ПАРСИМ DELETE
            if (upper_query.find("DELETE FROM") == 0) {
                // Теперь регулярка игнорирует алиасы (типа use.id) перед словом id
                std::regex re(R"(DELETE\s+FROM\s+(\w+).*?WHERE\s+.*?\bid\s*=\s*(\d+))", std::regex::icase);
                std::smatch match;
                if (std::regex_search(query, match, re)) {
                    return "DELETE " + match[1].str() + " " + match[2].str();
                }
            }

            // 4. ПАРСИМ UPDATE
            if (upper_query.find("UPDATE") == 0) {
                std::regex re(R"(UPDATE\s+(\w+)\s+SET\s+(.*?)\s+WHERE\s+id\s*=\s*(\d+))", std::regex::icase);
                std::smatch match;
                if (std::regex_search(query, match, re)) {
                    std::string table = match[1].str();
                    auto pairs = split(match[2].str(), ',');
                    std::string id = match[3].str();

                    std::string json = "{";
                    for (size_t i = 0; i < pairs.size(); ++i) {
                        auto kv = split(pairs[i], '=');
                        if (kv.size() == 2) {
                            std::string k = kv[0];
                            std::string v = kv[1];
                            if (!v.empty() && v.front() == '\'' && v.back() == '\'') {
                                v = "\"" + v.substr(1, v.size() - 2) + "\"";
                            }
                            json += "\"" + k + "\":" + v;
                            if (i < pairs.size() - 1) json += ",";
                        }
                    }
                    json += "}";
                    return "UPDATE " + table + " " + id + " " + json;
                }
            }
        } catch (const std::exception& e) {
            // Если регулярка сломается, сервер не упадет, а вернет ошибку клиенту!
            return std::string("ERR_SQL_PARSER: ") + e.what();
        }

        // Если это не SQL (например CREATE или SCHEMA) — отдаем как есть
        return query;
    }
}