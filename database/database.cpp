#include "database.h"
#include "../Orm/sql_parser.h"
#include <sstream>
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
        case WriteResult::invalid_data:    return "ERR_CONSTRAINT_VIOLATION";
        // Клиент не должен получить OK за операцию, которой нет в журнале:
        // после аварии она бы молча исчезла.
        case WriteResult::log_failed:      return "ERR_WAL_WRITE_FAILED";
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

    // Журнал очищаем, только если данные действительно на диске.
    // Иначе он остаётся, и при следующем запуске всё восстановится из него.
    if (!storage.flush_all()) {
        std::cerr << "[System] Flush failed, keeping WAL for recovery on next start" << std::endl;
        return;
    }
    if (!wal.reset()) {
        std::cerr << "[System] Could not clear WAL" << std::endl;
    }
}

// --- РЕАЛИЗАЦИЯ WAL ---

bool Database::log_write(const std::string& query) {
    // Во время восстановления журнал не дописываем: мы его как раз читаем.
    if (is_recovering) return true;
    return wal.append(query);
}

void Database::recover_from_wal() {
    const std::vector<std::string> records = wal.replay();
    if (records.empty()) return; // Журнал пуст — значит всё закрылось штатно

    std::cout << "\n[WAL] Crash detected! Found " << records.size()
              << " uncommitted operations." << std::endl;
    std::cout << "[WAL] Replaying log to restore Write Buffer..." << std::endl;

    // Часть операций могла успеть попасть на диск при автоматическом сбросе
    // блока. Их повтор безвреден: INSERT вернёт ERR_ID_EXISTS, а UPDATE и
    // DELETE приведут запись к тому же итоговому состоянию.
    is_recovering = true;
    for (const std::string& query : records) {
        execute(query); // Скармливаем команду базе, будто её прислал клиент
    }
    is_recovering = false;

    std::cout << "[WAL] Successfully recovered " << records.size()
              << " operations!\n" << std::endl;
}

void Database::load_config(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cout << "[Config] No setup.yaml found, skipping auto-init." << std::endl;
        return;
    }

    std::string line, current_table;
    std::vector<Column> current_cols;
    bool in_schema = false; // Флаг: находимся ли мы внутри блока schema:

    while (std::getline(file, line)) {
        // Пропускаем совсем пустые строки
        if (line.find_first_not_of(" \t\n\r") == std::string::npos) continue;

        // 1. Проверяем отступ (количество пробелов в начале)
        size_t indent = line.find_first_not_of(' ');

        // Глобальная настройка: насколько надёжно журнал проталкивается на диск.
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

        // 2. Если нашли "- name:" с МАЛЕНЬКИМ отступом (обычно 2) — это новая ТАБЛИЦА
        if (line.find("- name:") != std::string::npos && indent < 4) {
            // Сохраняем предыдущую таблицу перед переключением
            if (!current_table.empty() && !current_cols.empty()) {
                storage.create_table(current_table);
                storage.set_schema(current_table, current_cols);
                std::cout << "[Config] Table '" << current_table << "' initialized from YAML." << std::endl;
            }
            
            current_table = trim_cmd(line.substr(line.find(':') + 1));
            current_cols.clear();
            in_schema = false; // Сбрасываем флаг схемы, так как началась новая таблица
        } 
        // 3. Если встретили ключевое слово "schema:" — переходим в режим чтения колонок
        else if (line.find("schema:") != std::string::npos) {
            in_schema = true;
        }
        // 4. Если мы в режиме схемы и видим строку, начинающуюся с "- " — это КОЛОНКА
        else if (in_schema && line.find("- ") != std::string::npos) {
            size_t colon = line.find(':');
            if (colon != std::string::npos) {
                // Извлекаем имя колонки (между "- " и ":")
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

    // Сохраняем самую последнюю таблицу из файла (она всегда выводится здесь)
    if (!current_table.empty() && !current_cols.empty()) {
        storage.create_table(current_table);
        storage.set_schema(current_table, current_cols);
        std::cout << "[Config] Table '" << current_table << "' initialized from YAML." << std::endl;
    }
}
std::string Database::execute(const std::string& raw_query) {
    // 1. Прогоняем сырой запрос через наш SQL Транслятор
    std::string query = SQLParser::translate(raw_query);

    if (query.find("ERR") == 0) return query;
    
    std::stringstream ss(query);
    std::string cmd, table_name;
    
    if (!(ss >> cmd)) return "ERR_EMPTY_QUERY";
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    // 0. FLUSH — контрольная точка: буферы на диск, fsync, очистка журнала.
    if (cmd == "FLUSH") {
        std::unique_lock<std::shared_mutex> lock(checkpoint_mutex);
        if (!storage.flush_all()) return "ERR_FLUSH_FAILED";
        // Данные на диске — черновик можно сжечь.
        if (!wal.reset()) return "ERR_WAL_RESET_FAILED";
        return "OK: All buffers flushed to disk";
    }

    // Извлекаем имя таблицы (делаем это ДО блокировок, чтобы не тормозить потоки)
    if (!(ss >> table_name)) return "ERR_MISSING_TABLE_NAME";
    table_name = trim_cmd(table_name); 

    // 1. CREATE (DDL)
    if (cmd == "CREATE") {
        std::unique_lock<std::shared_mutex> lock(checkpoint_mutex);
        if (storage.create_table(table_name)) {
            return "OK: Table '" + table_name + "' created";
        }
        return "ERR_TABLE_ALREADY_EXISTS";
    }

    // 2. SCHEMA (DDL)
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
        if (storage.set_schema(table_name, cols)) return "OK: Schema applied";
        return "ERR_TABLE_NOT_FOUND";
    }

    // 3. SELECT — без замка контрольной точки.
    // Storage защищает каждую таблицу разделяемым замком, поэтому чтения
    // разных и одной и той же таблицы идут параллельно, а запись в таблицу
    // ждёт только читателей этой таблицы.
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

    // Журнал вызывается изнутри Storage под замком таблицы — после проверки
    // данных и до их применения (см. Storage::insert).
    const WriteLog log = [this, &query] { return log_write(query); };

    // 4. INSERT / UPDATE
    if (cmd == "INSERT" || cmd == "UPDATE") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";
        std::string body;
        std::getline(ss >> std::ws, body);
        body = trim_cmd(body);
        
        if (body.empty()) return "ERR_EMPTY_BODY";

        // Разделяемо: записи в разные таблицы не ждут друг друга.
        std::shared_lock<std::shared_mutex> lock(checkpoint_mutex);

        const WriteMode mode = (cmd == "INSERT") ? WriteMode::insert : WriteMode::upsert;
        return to_response(storage.insert(table_name, id, body, mode, log));
    }

    // 5. DELETE
    if (cmd == "DELETE") {
        int id;
        if (!(ss >> id)) return "ERR_INVALID_ID";

        std::shared_lock<std::shared_mutex> lock(checkpoint_mutex);
        return to_response(storage.remove(table_name, id, log));
    }

    return "ERR_UNKNOWN_COMMAND";
}
