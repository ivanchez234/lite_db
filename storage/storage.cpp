#include "storage.h"
#include <iostream>
#include <map>
#include <sstream>
#include <algorithm>
#include <fstream>
#include <cstring> // Для memcpy

Storage::Storage() {
    if (!fs::exists(root_path)) {
        fs::create_directories(root_path);
    }
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

std::vector<char> compress_block(const std::vector<char>& raw_data) {
    int max_compressed_size = LZ4_compressBound(raw_data.size());
    std::vector<char> compressed_buffer(max_compressed_size);

    int actual_size = LZ4_compress_default(
        raw_data.data(), compressed_buffer.data(), 
        raw_data.size(), max_compressed_size
    );

    compressed_buffer.resize(actual_size);
    return compressed_buffer;
}

void Storage::insert_to_block(Table* t, const std::vector<char>& raw_record) {
    uint32_t rec_sz = static_cast<uint32_t>(raw_record.size());
    t->write_buffer.insert(t->write_buffer.end(), reinterpret_cast<char*>(&rec_sz), reinterpret_cast<char*>(&rec_sz) + sizeof(rec_sz));
    t->write_buffer.insert(t->write_buffer.end(), raw_record.begin(), raw_record.end());

    if (t->write_buffer.size() >= t->BLOCK_SIZE) {
        flush_block_to_disk(t);
    }
}

void Storage::flush_block_to_disk(Table* t) {
    if (t->write_buffer.empty()) return;

    std::string path = t->path + "seg_" + std::to_string(t->current_seg_id) + ".db";
    std::ofstream file(path, std::ios::binary | std::ios::app);
    std::streampos block_start = file.tellp(); 

    std::vector<char> compressed = compress_block(t->write_buffer);

    CompressedBlockHeader header;
    header.original_size = t->write_buffer.size();
    header.compressed_size = compressed.size();

    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.write(compressed.data(), compressed.size());

    size_t offset = 0;
    while (offset < t->write_buffer.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, t->write_buffer.data() + offset, sizeof(uint32_t));
        int id;
        memcpy(&id, t->write_buffer.data() + offset + sizeof(uint32_t), sizeof(int));
        
        if (rec_sz == sizeof(int)) {
            t->index.erase(id); // Удаляем, если это Надгробие
        } else {
            t->index[id] = { path, block_start };
        }
        offset += sizeof(uint32_t) + rec_sz;
    }

    file.close();
    t->write_buffer.clear(); 
}

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
            k.erase(0, k.find_first_not_of(" ")); k.erase(k.find_last_not_of(" ") + 1);
            v.erase(0, v.find_first_not_of(" ")); v.erase(v.find_last_not_of(" ") + 1);
            res[k] = v;
        }
    }
    return res;
}

bool is_valid_date(const std::string& date) {
    if (date.length() != 10) return false;
    if (date[4] != '-' || date[7] != '-') return false;
    for (int i = 0; i < 10; ++i) {
        if (i == 4 || i == 7) continue;
        if (!std::isdigit(date[i])) return false;
    }
    int month = std::stoi(date.substr(5, 2));
    int day = std::stoi(date.substr(8, 2));
    if (month < 1 || month > 12) return false;
    if (day < 1 || day > 31) return false; 
    return true;
}

bool Storage::validate_types(Table* t, const std::map<std::string, std::string>& data) {
    if (t->schema.empty()) return true; 

    for (const auto& col : t->schema) {
        auto it = data.find(col.name);
        if (it == data.end()) return false; 

        const std::string& val = it->second;
        try {
            if (col.type == DataType::INT) std::stoi(val);
            else if (col.type == DataType::DOUBLE) std::stod(val);
            else if (col.type == DataType::BOOL) {
                if (val != "true" && val != "false" && val != "1" && val != "0") return false;
            }
            else if (col.type == DataType::DATE) {
                if (!is_valid_date(val)) return false;
            }
        } catch (...) {
            return false;
        }
    }
    return true;
}

std::vector<char> Storage::pack_json(const std::string& json_str, Table* t) {
    auto data = parse_json_manual(json_str);
    if (!validate_types(t, data)) throw std::runtime_error("ERR_CONSTRAINT_VIOLATION");

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
    if (tables.count(name)) return false;

    std::string t_path = root_path + name + "/";
    if (!fs::exists(t_path)) fs::create_directories(t_path);

    Table* t = new Table();
    t->name = name;
    t->path = t_path;
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
    save_schema(t); 
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
    
    std::vector<char> bin = pack_json(json_str, t);
    std::vector<char> full_record;
    full_record.insert(full_record.end(), reinterpret_cast<char*>(&id), reinterpret_cast<char*>(&id) + sizeof(int));
    full_record.insert(full_record.end(), bin.begin(), bin.end());

    insert_to_block(t, full_record);
}

std::string Storage::select(const std::string& table_name, int id, const std::string& target_key) {
    Table* t = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables_mtx);
        if (!tables.count(table_name)) return "ERR_TABLE_NOT_FOUND";
        t = tables[table_name];
    }
    std::lock_guard<std::mutex> t_lock(t->mtx);
    
    auto extract = [&](char* data_ptr) -> std::string {
        BinaryHeader head;
        memcpy(&head, data_ptr, sizeof(BinaryHeader));
        std::string to_find = target_key.empty() ? "__full__" : target_key;
        uint32_t h = hash_string(to_find);
        size_t keys_offset = sizeof(BinaryHeader);
        for (uint32_t i = 0; i < head.keys_count; ++i) {
            KeyRecord r;
            memcpy(&r, data_ptr + keys_offset + (i * sizeof(KeyRecord)), sizeof(KeyRecord));
            if (r.key_hash == h) {
                size_t val_offset = keys_offset + (head.keys_count * sizeof(KeyRecord)) + r.data_offset;
                return std::string(data_ptr + val_offset, r.data_size);
            }
        }
        return "ERR_KEY_NOT_FOUND";
    };

    size_t buf_off = 0;
    while (buf_off < t->write_buffer.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, t->write_buffer.data() + buf_off, sizeof(uint32_t));
        int rid;
        memcpy(&rid, t->write_buffer.data() + buf_off + sizeof(uint32_t), sizeof(int));
        
        if (rid == id) {
            if (rec_sz == sizeof(int)) return "ERR_NOT_FOUND"; // Проверка на Надгробие
            return extract(t->write_buffer.data() + buf_off + sizeof(uint32_t) + sizeof(int));
        }
        buf_off += sizeof(uint32_t) + rec_sz;
    }

    if (t->index.find(id) == t->index.end()) return "ERR_NOT_FOUND";
    FileLocation loc = t->index[id];

    std::ifstream in(loc.filename, std::ios::binary);
    in.seekg(loc.offset);

    CompressedBlockHeader header;
    in.read((char*)&header, sizeof(header));
    std::vector<char> comp(header.compressed_size);
    in.read(comp.data(), header.compressed_size);

    std::vector<char> orig(header.original_size);
    LZ4_decompress_safe(comp.data(), orig.data(), header.compressed_size, header.original_size);

    size_t offset = 0;
    while (offset < orig.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, orig.data() + offset, sizeof(uint32_t));
        int rid;
        memcpy(&rid, orig.data() + offset + sizeof(uint32_t), sizeof(int));

        if (rid == id) {
            if (rec_sz == sizeof(int)) return "ERR_NOT_FOUND"; // Проверка на Надгробие
            return extract(orig.data() + offset + sizeof(uint32_t) + sizeof(int));
        }
        offset += sizeof(uint32_t) + rec_sz;
    }
    return "ERR_NOT_FOUND";
}

std::string Storage::select_all(const std::string& table_name) {
    Table* t = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables_mtx);
        if (!tables.count(table_name)) return "ERR_TABLE_NOT_FOUND";
        t = tables[table_name];
    }
    std::lock_guard<std::mutex> t_lock(t->mtx);
    
    std::map<int, std::string> latest_data;

    auto extract = [&](char* data_ptr) -> std::string {
        BinaryHeader head;
        memcpy(&head, data_ptr, sizeof(BinaryHeader));
        uint32_t h = hash_string("__full__");
        size_t keys_offset = sizeof(BinaryHeader);
        for (uint32_t i = 0; i < head.keys_count; ++i) {
            KeyRecord r;
            memcpy(&r, data_ptr + keys_offset + (i * sizeof(KeyRecord)), sizeof(KeyRecord));
            if (r.key_hash == h) return std::string(data_ptr + keys_offset + (head.keys_count * sizeof(KeyRecord)) + r.data_offset, r.data_size);
        }
        return "{}";
    };

    int sid = 0;
    while (fs::exists(t->path + "seg_" + std::to_string(sid) + ".db")) {
        std::ifstream in(t->path + "seg_" + std::to_string(sid) + ".db", std::ios::binary);
        while (in.peek() != EOF && in.good()) {
            CompressedBlockHeader header;
            if (!in.read((char*)&header, sizeof(header))) break;

            std::vector<char> comp(header.compressed_size);
            in.read(comp.data(), header.compressed_size);

            std::vector<char> orig(header.original_size);
            LZ4_decompress_safe(comp.data(), orig.data(), header.compressed_size, header.original_size);

            size_t offset = 0;
            while (offset < orig.size()) {
                uint32_t rec_sz;
                memcpy(&rec_sz, orig.data() + offset, sizeof(uint32_t));
                int id;
                memcpy(&id, orig.data() + offset + sizeof(uint32_t), sizeof(int));
                
                if (rec_sz == sizeof(int)) {
                    latest_data.erase(id);
                } else {
                    latest_data[id] = extract(orig.data() + offset + sizeof(uint32_t) + sizeof(int));
                }
                offset += sizeof(uint32_t) + rec_sz;
            }
        }
        sid++;
    }

    size_t buf_off = 0;
    while (buf_off < t->write_buffer.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, t->write_buffer.data() + buf_off, sizeof(uint32_t));
        int id;
        memcpy(&id, t->write_buffer.data() + buf_off + sizeof(uint32_t), sizeof(int));
        
        if (rec_sz == sizeof(int)) {
            latest_data.erase(id); 
        } else {
            latest_data[id] = extract(t->write_buffer.data() + buf_off + sizeof(uint32_t) + sizeof(int));
        }
        buf_off += sizeof(uint32_t) + rec_sz;
    }

    if (latest_data.empty()) return "[]";

    std::string result = "[\n";
    bool first = true;
    for (const auto& [id, json_str] : latest_data) {
        if (!first) result += ",\n";
        result += "  { \"id\": " + std::to_string(id) + ", \"data\": " + json_str + " }";
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
        
        while (in.peek() != EOF && in.good()) {
            std::streampos block_start = in.tellg();
            CompressedBlockHeader header;
            if (!in.read((char*)&header, sizeof(header))) break;
            
            std::vector<char> comp(header.compressed_size);
            in.read(comp.data(), header.compressed_size);
            
            std::vector<char> orig(header.original_size);
            LZ4_decompress_safe(comp.data(), orig.data(), header.compressed_size, header.original_size);
            
            size_t offset = 0;
            while (offset < orig.size()) {
                uint32_t rec_sz;
                memcpy(&rec_sz, orig.data() + offset, sizeof(uint32_t));
                int id;
                memcpy(&id, orig.data() + offset + sizeof(uint32_t), sizeof(int));
                
                if (rec_sz == sizeof(int)) {
                    t->index.erase(id);
                } else {
                    t->index[id] = { fn, block_start };
                }
                offset += sizeof(uint32_t) + rec_sz;
            }
        }
        sid++;
    }
    t->current_seg_id = std::max(0, sid - 1);
}

bool Storage::exists(const std::string& table_name, int id) {
    std::lock_guard<std::mutex> lock(tables_mtx);
    if (!tables.count(table_name)) return false;
    Table* t = tables[table_name];
    std::lock_guard<std::mutex> t_lock(t->mtx);

    size_t buf_off = 0;
    bool found_in_buf = false;
    bool is_deleted = false;
    
    while (buf_off < t->write_buffer.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, t->write_buffer.data() + buf_off, sizeof(uint32_t));
        int rid;
        memcpy(&rid, t->write_buffer.data() + buf_off + sizeof(uint32_t), sizeof(int));
        
        if (rid == id) {
            found_in_buf = true;
            is_deleted = (rec_sz == sizeof(int)); 
        }
        buf_off += sizeof(uint32_t) + rec_sz;
    }

    if (found_in_buf) return !is_deleted;
    return t->index.find(id) != t->index.end();
}

void Storage::remove(const std::string& table_name, int id) {
    std::lock_guard<std::mutex> lock(tables_mtx);
    if (!tables.count(table_name)) return;
    Table* t = tables[table_name];
    std::lock_guard<std::mutex> t_lock(t->mtx);

    t->index.erase(id); 

    std::vector<char> tombstone;
    tombstone.insert(tombstone.end(), reinterpret_cast<char*>(&id), reinterpret_cast<char*>(&id) + sizeof(int));
    insert_to_block(t, tombstone);
}