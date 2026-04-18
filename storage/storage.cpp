#include "storage.h"
#include <iostream>
#include <map>
#include <sstream>
#include <algorithm>

Storage::Storage() {
    if (!fs::exists(root_path)) {
        fs::create_directories(root_path);
    }
    
    // Сканируем папку data/ на наличие таблиц
    for (const auto& entry : fs::directory_iterator(root_path)) {
        if (entry.is_directory()) {
            std::string t_name = entry.path().filename().string();
            create_table(t_name); 
        }
    }
}

Storage::~Storage() {
    std::lock_guard<std::mutex> lock(tables_mtx);
    for (auto& [name, table] : tables) {
        delete table;
    }
}

uint32_t Storage::hash_string(const std::string& s) {
    uint32_t hash = 5381;
    for (char c : s) hash = ((hash << 5) + hash) + c;
    return hash;
}

std::map<std::string, std::string> Storage::parse_json_manual(std::string s) {
    std::map<std::string, std::string> res;
    // Очистка от скобок и кавычек
    s.erase(std::remove(s.begin(), s.end(), '{'), s.end());
    s.erase(std::remove(s.begin(), s.end(), '}'), s.end());
    s.erase(std::remove(s.begin(), s.end(), '\"'), s.end());

    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        size_t colon = item.find(':');
        if (colon != std::string::npos) {
            std::string k = item.substr(0, colon);
            std::string v = item.substr(colon + 1);
            // Trim пробелов
            k.erase(0, k.find_first_not_of(" ")); k.erase(k.find_last_not_of(" ") + 1);
            v.erase(0, v.find_first_not_of(" ")); v.erase(v.find_last_not_of(" ") + 1);
            res[k] = v;
        }
    }
    return res;
}

bool is_valid_date(const std::string& date) {
    // 1. Проверяем длину: ровно 10 символов (YYYY-MM-DD)
    if (date.length() != 10) return false;

    // 2. Проверяем наличие дефисов на правильных местах
    if (date[4] != '-' || date[7] != '-') return false;

    // 3. Проверяем, что остальные символы — это цифры
    for (int i = 0; i < 10; ++i) {
        if (i == 4 || i == 7) continue;
        if (!std::isdigit(date[i])) return false;
    }

    // 4. Логическая проверка месяцев и дней
    int month = std::stoi(date.substr(5, 2));
    int day = std::stoi(date.substr(8, 2));

    if (month < 1 || month > 12) return false;
    if (day < 1 || day > 31) return false; 
    // Для идеала можно добавить проверку на 28/30 дней, но пока хватит и этого

    return true;
}

// Валидация типов данных (Constraints)
bool Storage::validate_types(Table* t, const std::map<std::string, std::string>& data) {
    if (t->schema.empty()) return true; // Если схема не задана, пропускаем всё

    for (const auto& col : t->schema) {
        // Проверка на наличие обязательного поля
        auto it = data.find(col.name);
        if (it == data.end()) return false; 

        const std::string& val = it->second;
        try {
            if (col.type == DataType::INT) {
                std::stoi(val);
            } else if (col.type == DataType::DOUBLE) {
                std::stod(val);
            } else if (col.type == DataType::BOOL) {
                if (val != "true" && val != "false" && val != "1" && val != "0") return false;
            }
            else if (col.type == DataType::DATE)
            {
                if (!is_valid_date(val))
                {
                    return false;
                }
            }
            // STRING валидировать не нужно
        } catch (...) {
            return false; // Ошибка трансформации типа
        }
    }
    return true;
}

// Упаковка данных в бинарный блок с учетом схемы
std::vector<char> Storage::pack_json(const std::string& json_str, Table* t) {
    auto data = parse_json_manual(json_str);
    
    // Перед упаковкой проверяем типы
    if (!validate_types(t, data)) {
        throw std::runtime_error("ERR_CONSTRAINT_VIOLATION");
    }

    data["__full__"] = json_str; 

    BinaryHeader head;
    head.keys_count = (uint32_t)data.size();
    
    std::vector<KeyRecord> records;
    std::vector<char> body;
    uint32_t offset = 0;

    for (auto const& [k, v] : data) {
        KeyRecord r;
        r.key_hash = hash_string(k);
        r.data_offset = offset;
        r.data_size = (uint32_t)v.size();
        records.push_back(r);
        body.insert(body.end(), v.begin(), v.end());
        offset += (uint32_t)v.size();
    }

    head.total_size = sizeof(BinaryHeader) + (uint32_t)(records.size() * sizeof(KeyRecord)) + (uint32_t)body.size();

    std::vector<char> buf;
    buf.insert(buf.end(), (char*)&head, (char*)&head + sizeof(BinaryHeader));
    for (auto& r : records) buf.insert(buf.end(), (char*)&r, (char*)&r + sizeof(KeyRecord));
    buf.insert(buf.end(), body.begin(), body.end());
    return buf;
}

bool Storage::create_table(const std::string& name) {
    std::lock_guard<std::mutex> lock(tables_mtx);
    
    // Если таблица уже есть в памяти — выходим
    if (tables.count(name)) return false;

    std::string t_path = root_path + name + "/";
    if (!fs::exists(t_path)) {
        fs::create_directories(t_path);
    }

    Table* t = new Table();
    t->name = name;
    t->path = t_path;
    
    // ВАЖНО: сначала добавляем в список, потом загружаем остальное
    tables[name] = t; 
    
    load_schema(t);
    load_table_index(t);
    
    return true;
}

bool Storage::set_schema(const std::string& table_name, const std::vector<Column>& columns) {
    std::lock_guard<std::mutex> lock(tables_mtx);
    if (!tables.count(table_name)) return false;
    
    Table* t = tables[table_name];
    t->schema = columns;
    save_schema(t); // Сохраняем схему в _schema.bin
    return true;
}

void Storage::save_schema(Table* t) {
    std::ofstream out(t->path + "_schema.bin", std::ios::binary);
    uint32_t col_count = (uint32_t)t->schema.size();
    out.write((char*)&col_count, sizeof(uint32_t));

    for (const auto& col : t->schema) {
        uint32_t name_len = (uint32_t)col.name.size();
        out.write((char*)&name_len, sizeof(uint32_t));
        out.write(col.name.c_str(), name_len);
        out.write((char*)&col.type, sizeof(DataType));
    }
}

void Storage::load_schema(Table* t) {
    std::ifstream in(t->path + "_schema.bin", std::ios::binary);
    if (!in.is_open()) return;

    uint32_t col_count;
    if (!in.read((char*)&col_count, sizeof(uint32_t))) return;

    t->schema.clear();
    for (uint32_t i = 0; i < col_count; ++i) {
        uint32_t name_len;
        in.read((char*)&name_len, sizeof(uint32_t));
        std::string name(name_len, ' ');
        in.read(&name[0], name_len);
        DataType type;
        in.read((char*)&type, sizeof(DataType));
        t->schema.push_back({name, type});
    }
}

void Storage::insert(const std::string& table_name, int id, const std::string& json_str) {
    Table* t = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables_mtx);
        if (!tables.count(table_name)) return;
        t = tables[table_name];
    }

    std::lock_guard<std::mutex> t_lock(t->mtx);
    
    // 1. Прямой вызов. Если типы не совпадут, pack_json сам кинет исключение.
    std::vector<char> bin = pack_json(json_str, t);

    // 2. Запись на диск
    std::string fname = t->path + "seg_" + std::to_string(t->current_seg_id) + ".db";
    std::ofstream out(fname, std::ios::binary | std::ios::app); // Используем app для простоты

    if (!out.is_open()) throw std::runtime_error("Could not open file for writing");

    std::streampos pos = out.tellp(); 

    uint32_t len = (uint32_t)bin.size();
    out.write((char*)&id, sizeof(int));
    out.write((char*)&len, sizeof(uint32_t));
    out.write(bin.data(), bin.size());
    
    out.flush();
    out.close(); // <--- ОБЯЗАТЕЛЬНО закрываем, чтобы SELECT мог прочитать

    // 3. Обновляем индекс только ПОСЛЕ успешной записи
    t->index[id] = { fname, pos };

    if (pos > (std::streamoff)MAX_SEG_SIZE) {
        t->current_seg_id++;
    }
}

std::string Storage::select(const std::string& table_name, int id, const std::string& target_key) {
    Table* t = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables_mtx);
        if (!tables.count(table_name)) return "ERR_TABLE_NOT_FOUND";
        t = tables[table_name];
    }

    FileLocation loc;
    {
        std::lock_guard<std::mutex> t_lock(t->mtx);
        if (t->index.find(id) == t->index.end()) return "ERR_NOT_FOUND";
        loc = t->index[id];
    }

    std::ifstream in(loc.filename, std::ios::binary);
    in.seekg(loc.offset);

    int rid; uint32_t dlen;
    in.read((char*)&rid, sizeof(int));
    in.read((char*)&dlen, sizeof(uint32_t));

    BinaryHeader head;
    in.read((char*)&head, sizeof(BinaryHeader));
    
    std::string to_find = target_key.empty() ? "__full__" : target_key;
    uint32_t h = hash_string(to_find);

    for (uint32_t i = 0; i < head.keys_count; ++i) {
        KeyRecord r;
        in.read((char*)&r, sizeof(KeyRecord));
        if (r.key_hash == h) {
        // Смещение данных: начало записи + заголовок + массив записей ключей + смещение конкретного ключа
        size_t header_offset = sizeof(int) + sizeof(uint32_t) + sizeof(BinaryHeader) + (head.keys_count * sizeof(KeyRecord));
        in.seekg((size_t)loc.offset + header_offset + r.data_offset);
        
        std::string res(r.data_size, ' ');
        in.read(&res[0], r.data_size);
        return res;
        }
    }
    return "ERR_KEY_NOT_FOUND";
}

std::string Storage::select_all(const std::string& table_name) {
    Table* t = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables_mtx);
        if (!tables.count(table_name)) return "ERR_TABLE_NOT_FOUND";
        t = tables[table_name];
    }

    // ВАЖНО: Мы НЕ блокируем t->mtx здесь, 
    // потому что метод select() ниже сделает это сам для каждой записи.
    
    if (t->index.empty()) return "[]";

    std::string result = "[\n";
    bool first = true;

    // Делаем копию индекса или просто проходим по нему, 
    // так как чтение структуры индекса обычно безопасно.
    for (auto const& [id, loc] : t->index) {
        if (!first) result += ",\n";
        
        // select сам заблокирует мьютекс, прочитает данные и разблокирует.
        std::string record = select(table_name, id, ""); 
        result += "  { \"id\": " + std::to_string(id) + ", \"data\": " + record + " }";
        first = false;
    }

    result += "\n]";
    return result;
}
void Storage::load_table_index(Table* t) {
    int sid = 0;
    while (fs::exists(t->path + "seg_" + std::to_string(sid) + ".db")) {
        std::string fn = t->path + "seg_" + std::to_string(sid) + ".db";
        std::ifstream in(fn, std::ios::binary);
        while (in.peek() != EOF) {
            std::streampos p = in.tellg();
            int id; uint32_t len;
            if (!in.read((char*)&id, sizeof(int))) break;
            if (!in.read((char*)&len, sizeof(uint32_t))) break;
            t->index[id] = { fn, p };
            in.seekg(len, std::ios::cur);
        }
        sid++;
    }
    t->current_seg_id = std::max(0, sid - 1);
}

bool Storage::exists(const std::string& table_name, int id) {
    std::lock_guard<std::mutex> lock(tables_mtx);
    if (!tables.count(table_name)) return false;
    auto& idx = tables[table_name]->index;
    return idx.find(id) != idx.end();
}

void Storage::remove(const std::string& table_name, int id) {
    std::lock_guard<std::mutex> lock(tables_mtx);
    if (tables.count(table_name)) {
        std::lock_guard<std::mutex> t_lock(tables[table_name]->mtx);
        tables[table_name]->index.erase(id);
    }
}