#include "database.h"
#include "../Orm/sql_parser.h"
#include <sstream>
#include <cctype>
#include <string_view>
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
 
namespace {
 
std::string to_response(WriteResult result) {
    switch (result) {
        case WriteResult::ok:              return "OK";
        case WriteResult::table_not_found: return "ERR_TABLE_NOT_FOUND";
        case WriteResult::id_exists:       return "ERR_ID_EXISTS";
        case WriteResult::not_found:       return "ERR_NOT_FOUND";
        case WriteResult::invalid_json:    return "ERR_INVALID_JSON";
        case WriteResult::invalid_data:    return "ERR_CONSTRAINT_VIOLATION";
        case WriteResult::read_error:      return "ERR_READ_FAILED";

        case WriteResult::log_failed:      return "ERR_WAL_WRITE_FAILED";
        case WriteResult::field_hash_collision:
            return "ERR_FIELD_HASH_COLLISION: two field names have the same hash, rename one of them";
    }
    return "ERR_INTERNAL";
}
 
std::string to_response(SchemaResult result) {
    switch (result) {
        case SchemaResult::ok:               return "OK: Schema applied";
        case SchemaResult::table_not_found:  return "ERR_TABLE_NOT_FOUND";
        case SchemaResult::duplicate_column: return "ERR_DUPLICATE_COLUMN";
        case SchemaResult::hash_collision:
            return "ERR_SCHEMA_HASH_COLLISION: two column names have the same hash, rename one of them";
    }
    return "ERR_INTERNAL";
}
 
} // namespace
 
Database::Database(const std::string& wal_path)
    : wal(wal_path) {
    if (!wal.is_open()) {
        std::cerr << "WARNING: Could not open " << wal_path
                  << " for writing! All writes will be rejected." << std::endl;
    }
}
 
Database::~Database() {
    std::cout << "[System] Flushing buffers to disk before shutdown..." << std::endl;
    std::unique_lock<std::shared_mutex> lock(checkpoint_mutex);
 
    if (!storage.flush_all()) {
        std::cerr << "[System] Flush failed, keeping WAL for recovery on next start" << std::endl;
        return;
    }
    if (!wal.reset()) {
        std::cerr << "[System] Could not clear WAL" << std::endl;
    }
}
 
// реализация .wal
 
bool Database::log_write(const std::string& query) {
    if (is_recovering) return true;
    return wal.append(query);
}
 
void Database::recover_from_wal() {
    const std::vector<std::string> records = wal.replay();
    if (records.empty()) return; 
 
    std::cout << "\n[WAL] Crash detected! Found " << records.size()
              << " uncommitted operations." << std::endl;
    std::cout << "[WAL] Replaying log to restore Write Buffer..." << std::endl;
 
    is_recovering = true;
    for (const std::string& query : records) {
        execute(query);
    }
    is_recovering = false;
 
    std::cout << "[WAL] Successfully recovered " << records.size()
              << " operations!\n" << std::endl;
}
 
void Database::load_config(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::error_code ec;
        const auto where = fs::absolute(filename, ec);
        std::cout << "[Config] " << (ec ? filename : where.string())
                  << " not found - starting without tables from config" << std::endl;
        return;
    }
 
    std::string line, current_table;
    std::vector<Column> current_cols;
    bool in_schema = false; 
    const auto init_table = [this](const std::string& name, const std::vector<Column>& cols) {
        storage.create_table(name);
        const SchemaResult result = storage.set_schema(name, cols);
        if (result == SchemaResult::ok) {
            std::cout << "[Config] Table '" << name << "' initialized from YAML." << std::endl;
        } else {
            std::cout << "[Config] Table '" << name << "': schema rejected - "
                      << to_response(result) << std::endl;
        }
    };
 
    while (std::getline(file, line)) {
        if (line.find_first_not_of(" \t\n\r") == std::string::npos) continue;
 
        size_t indent = line.find_first_not_of(' ');
 
        if (indent == 0 && line.find("block_cache_mb:") == 0) {
            try {
                const long long mb = std::stoll(trim_cmd(line.substr(line.find(':') + 1)));
                if (mb < 0) throw std::out_of_range("negative");
                storage.set_block_cache_bytes(static_cast<size_t>(mb) * 1024u * 1024u);
                std::cout << "[Config] Block cache: " << mb << " MB" << std::endl;
            } catch (...) {
                std::cout << "[Config] Bad block_cache_mb, keeping default" << std::endl;
            }
            in_schema = false;
            continue;
        }
 
        if (indent == 0 && line.find("block_size_kb:") == 0) {
            try {
                const long long kb = std::stoll(trim_cmd(line.substr(line.find(':') + 1)));
                if (kb < 1 || kb > 16 * 1024) throw std::out_of_range("block size");
                storage.set_block_size(static_cast<size_t>(kb) * 1024u);
                std::cout << "[Config] Block size: " << kb << " KB" << std::endl;
            } catch (...) {
                std::cout << "[Config] Bad block_size_kb (1..16384), keeping default" << std::endl;
            }
            in_schema = false;
            continue;
        }
 
        if (indent == 0 && line.find("wal_sync:") != std::string::npos) {
            const std::string value = trim_cmd(line.substr(line.find(':') + 1));
            Wal::Sync mode;
            if (Wal::parse_sync_mode(value, mode)) {
                wal.set_sync_mode(mode);
                std::cout << "[Config] WAL sync mode: " << value << std::endl;
            } else {
                std::cout << "[Config] Unknown wal_sync '" << value << "', keeping "
                          << Wal::sync_mode_name(wal.sync_mode()) << std::endl;
            }
            in_schema = false;
            continue;
        }
 
        if (line.find("- name:") != std::string::npos && indent < 4) {
            if (!current_table.empty() && !current_cols.empty()) {
                init_table(current_table, current_cols);
            }
            
            current_table = trim_cmd(line.substr(line.find(':') + 1));
            current_cols.clear();
            in_schema = false; 
        } 
        else if (line.find("schema:") != std::string::npos) {
            in_schema = true;
        }
        else if (in_schema && line.find("- ") != std::string::npos) {
            size_t colon = line.find(':');
            if (colon != std::string::npos) {
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
 
    if (!current_table.empty() && !current_cols.empty()) {
        init_table(current_table, current_cols);
    }
}
std::string Database::execute(const std::string& raw_query) {
    std::string query = SQLParser::translate(raw_query);
 
    if (query.find("ERR") == 0) return query;
    
    std::stringstream ss(query);
    std::string cmd, table_name;
    
    if (!(ss >> cmd)) return "ERR_EMPTY_QUERY";
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
 
    if (cmd == "FLUSH") {
        std::unique_lock<std::shared_mutex> lock(checkpoint_mutex);
        if (!storage.flush_all()) return "ERR_FLUSH_FAILED";
        // Данные на диске — черновик можно сжечь.
        if (!wal.reset()) return "ERR_WAL_RESET_FAILED";
        return "OK: All buffers flushed to disk";
    }
 
    if (cmd == "STATS") {
        const StorageStats& s = storage.stats();
        return "{\"wal_appends\":"            + std::to_string(wal.appends())
             + ",\"wal_fsyncs\":"             + std::to_string(wal.fsyncs())
             + ",\"wal_bytes\":"              + std::to_string(wal.bytes())
             + ",\"blocks_written\":"         + std::to_string(s.blocks_written.get())
             + ",\"block_bytes_raw\":"        + std::to_string(s.block_bytes_raw.get())
             + ",\"block_bytes_compressed\":" + std::to_string(s.block_bytes_compressed.get())
             + ",\"blocks_read\":"            + std::to_string(s.blocks_read.get())
             + ",\"cache_hits\":"             + std::to_string(storage.cache_hits())
             + ",\"cache_misses\":"           + std::to_string(storage.cache_misses())
             + "}";
    }
 
    if (!(ss >> table_name)) return "ERR_MISSING_TABLE_NAME";
    table_name = trim_cmd(table_name); 
 
    if (cmd == "CREATE") {
        std::unique_lock<std::shared_mutex> lock(checkpoint_mutex);
        if (storage.create_table(table_name)) {
            return "OK: Table '" + table_name + "' created";
        }
        return "ERR_TABLE_ALREADY_EXISTS";
    }
 
    if (cmd == "SCHEMA") {
        std::vector<Column> cols;
        std::string pair;
        
        // Парсим строку без блокировки
        while (ss >> pair) {
            size_t colon = pair.find(':');
            if (colon == std::string::npos) continue;
            
            std::string col_name = pair.substr(0, colon);
            std::string type_str = trim_cmd(pair.substr(colon + 1));
            
            DataType type = DataType::STRING;
            if (type_str == "INT") type = DataType::INT;
            else if (type_str == "DOUBLE") type = DataType::DOUBLE;
            else if (type_str == "BOOL") type = DataType::BOOL;
            else if (type_str == "DATE") type = DataType::DATE;
            
            cols.push_back({col_name, type});
        }
        
        if (cols.empty()) return "ERR_EMPTY_SCHEMA";
        
        std::unique_lock<std::shared_mutex> lock(checkpoint_mutex);
        return to_response(storage.set_schema(table_name, cols));
    }
 
    if (cmd == "SELECT") {
        std::string arg;
        if (!(ss >> arg)) return "ERR_MISSING_ARGUMENTS";
        
        std::string upper_arg = arg;
        std::transform(upper_arg.begin(), upper_arg.end(), upper_arg.begin(), ::toupper);
 
        if (upper_arg == "ALL" || upper_arg == "*") {
            return storage.select_all(table_name);
        }
 
        try {
            int id = std::stoi(arg); 
            std::string key;
            if (ss >> key) {
                key = trim_cmd(key);
            }
            return storage.select(table_name, id, key);
        } catch (...) {
            return "ERR_INVALID_ID_OR_COMMAND (Expected INT or ALL)";
        }
    }
 
    const WriteLog log = [this, &query] { return log_write(query); };
 
    if (cmd == "INSERT" || cmd == "UPDATE") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        std::string body;
        std::getline(ss >> std::ws, body);
        body = trim_cmd(body);
        
        if (body.empty()) return "ERR_EMPTY_BODY";
 
        std::shared_lock<std::shared_mutex> lock(checkpoint_mutex);
 
        const WriteMode mode = (cmd == "INSERT") ? WriteMode::insert : WriteMode::update;
        return to_response(storage.insert(table_name, id, body, mode, log));
    }
 
    if (cmd == "MPUT") {
        std::string rest;
        std::getline(ss, rest);
 
        Storage::BatchRows rows;
        size_t pos = 0;
        while (true) {
            while (pos < rest.size() && std::isspace(static_cast<unsigned char>(rest[pos]))) ++pos;
            if (pos >= rest.size()) break;
 
            const size_t id_start = pos;
            if (rest[pos] == '-') ++pos;
            while (pos < rest.size() && std::isdigit(static_cast<unsigned char>(rest[pos]))) ++pos;
            int id = 0;
            try {
                size_t used = 0;
                id = std::stoi(rest.substr(id_start, pos - id_start), &used);
                if (used != pos - id_start) return "ERR_INVALID_ID";
            } catch (...) {
                return "ERR_INVALID_ID";
            }
 
            lite_db::json::Fields fields;
            std::string error;
            size_t consumed = 0;
            if (!lite_db::json::parse_object_prefix(std::string_view(rest).substr(pos), consumed, fields, error)) {
                return "ERR_INVALID_JSON";
            }
            pos += consumed;
            rows.emplace_back(id, std::move(fields));
        }
 
        if (rows.empty()) return "ERR_EMPTY_BODY";
 
        std::shared_lock<std::shared_mutex> lock(checkpoint_mutex);
        return to_response(storage.insert_batch(table_name, rows, is_recovering, log));
    }
 
    if (cmd == "DELETE") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
 
        std::shared_lock<std::shared_mutex> lock(checkpoint_mutex);
        return to_response(storage.remove(table_name, id, log));
    }
 
    return "ERR_UNKNOWN_COMMAND";
}
 