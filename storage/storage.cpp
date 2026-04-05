#include "storage.h"
#include <iostream>
#include <map>
#include <sstream>
#include <algorithm>

// Конструктор: инициализирует корневую папку и загружает существующие таблицы
Storage::Storage() {
    if (!fs::exists(root_path)) {
        fs::create_directory(root_path);
    }
    
    // Сканируем директорию data/ на наличие подпапок (таблиц)
    for (const auto& entry : fs::directory_iterator(root_path)) {
        if (entry.is_directory()) {
            std::string t_name = entry.path().filename().string();
            // Загружаем таблицу. Если она уже на диске, create_table её подцепит
            create_table(t_name); 
        }
    }
}

// Деструктор: чистим динамическую память
Storage::~Storage() {
    std::lock_guard<std::mutex> lock(tables_mtx);
    for (auto& [name, table] : tables) {
        delete table;
    }
}

// Хеширование ключей (DJB2)
uint32_t Storage::hash_string(const std::string& s) {
    uint32_t hash = 5381;
    for (char c : s) hash = ((hash << 5) + hash) + c;
    return hash;
}

// Ручной парсер JSON (строка -> map)
std::map<std::string, std::string> Storage::parse_json_manual(std::string s) {
    std::map<std::string, std::string> res;
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
            // Убираем лишние пробелы (trim)
            k.erase(0, k.find_first_not_of(" ")); k.erase(k.find_last_not_of(" ") + 1);
            v.erase(0, v.find_first_not_of(" ")); v.erase(v.find_last_not_of(" ") + 1);
            res[k] = v;
        }
    }
    return res;
}

// Упаковка данных в бинарный формат
std::vector<char> Storage::pack_json(const std::string& json_str) {
    auto data = parse_json_manual(json_str);
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

// Создание новой таблицы (или регистрация существующей)
bool Storage::create_table(const std::string& name) {
    std::lock_guard<std::mutex> lock(tables_mtx);
    if (tables.count(name)) return false;

    std::string t_path = root_path + name + "/";
    if (!fs::exists(t_path)) {
        fs::create_directories(t_path);
    }

    Table* t = new Table();
    t->name = name;
    t->path = t_path;
    load_table_index(t); // Загружаем индекс именно этой таблицы
    
    tables[name] = t;
    return true;
}

// Вставка данных
void Storage::insert(const std::string& table_name, int id, const std::string& json_str) {
    Table* t = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables_mtx);
        if (!tables.count(table_name)) return;
        t = tables[table_name];
    }

    // Блокируем только конкретную таблицу
    std::lock_guard<std::mutex> t_lock(t->mtx);
    
    std::vector<char> bin = pack_json(json_str);
    std::string fname = t->path + "seg_" + std::to_string(t->current_seg_id) + ".db";

    std::ofstream out(fname, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    if (!out.is_open()) {
        out.open(fname, std::ios::binary | std::ios::out);
    }

    out.seekp(0, std::ios::end);
    std::streampos pos = out.tellp(); 

    uint32_t len = (uint32_t)bin.size();
    out.write((char*)&id, sizeof(int));
    out.write((char*)&len, sizeof(uint32_t));
    out.write(bin.data(), bin.size());
    out.flush(); 

    t->index[id] = { fname, pos };

    if (pos > (std::streamoff)MAX_SEG_SIZE) {
        t->current_seg_id++;
    }
}

// Поиск данных
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
    if (head.magic != 0x4A423031) return "ERR_CORRUPT_FORMAT";

    std::string to_find = target_key.empty() ? "__full__" : target_key;
    uint32_t h = hash_string(to_find);

    for (uint32_t i = 0; i < head.keys_count; ++i) {
        KeyRecord r;
        in.read((char*)&r, sizeof(KeyRecord));
        if (r.key_hash == h) {
            size_t p = (size_t)loc.offset + sizeof(int) + sizeof(uint32_t) + 
                       sizeof(BinaryHeader) + (head.keys_count * sizeof(KeyRecord)) + r.data_offset;
            in.seekg(p);
            std::string res(r.data_size, ' ');
            in.read(&res[0], r.data_size);
            return res;
        }
    }
    return "ERR_KEY_NOT_FOUND";
}

// Загрузка индекса для конкретной таблицы
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