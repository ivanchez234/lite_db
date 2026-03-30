#include "storage.h"
#include <iostream>
#include <map>
#include <sstream>
#include <algorithm>
#include <filesystem>

namespace fs = std::filesystem;

Storage::Storage() { load_index(); }

uint32_t Storage::hash_string(const std::string& s) {
    uint32_t hash = 5381;
    for (char c : s) hash = ((hash << 5) + hash) + c;
    return hash;
}

// Простой парсер: {"a":1} -> map["a"]="1"
std::map<std::string, std::string> parse_json_manual(std::string s) {
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

std::vector<char> Storage::pack_json(const std::string& json_str) {
    auto data = parse_json_manual(json_str);
    data["__full__"] = json_str; // Скрытый ключ для SELECT без параметров

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

void Storage::insert(int id, const std::string& json_str) {
    std::lock_guard<std::mutex> lock(mtx);
    
    std::vector<char> bin = pack_json(json_str);
    std::string fname = base_name + std::to_string(current_seg_id) + ".db";

    // Открываем БЕЗ ios::app, используем ios::ate (at the end), чтобы курсор сразу был в конце
    std::ofstream out(fname, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    
    if (!out.is_open()) {
        // Если файла нет, создаем новый
        out.open(fname, std::ios::binary | std::ios::out);
    }

    // Принудительно прыгаем в самый конец, чтобы получить актуальную позицию
    out.seekp(0, std::ios::end);
    std::streampos pos = out.tellp(); 

    uint32_t len = (uint32_t)bin.size();
    out.write((char*)&id, sizeof(int));
    out.write((char*)&len, sizeof(uint32_t));
    out.write(bin.data(), bin.size());
    out.flush(); // Сбрасываем буфер, чтобы данные точно были на диске

    // ОБНОВЛЯЕМ индекс. Если ID 10 уже был, старое значение затрется новым адресом.
    index[id] = { fname, pos };

    // Проверка на размер сегмента
    if (pos > (std::streamoff)MAX_SEG_SIZE) {
        current_seg_id++;
    }
}

std::string Storage::select(int id, const std::string& target_key) {
    FileLocation loc;
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (index.find(id) == index.end()) return "ERR_NOT_FOUND";
        loc = index[id];
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

bool Storage::exists(int id) {
    std::lock_guard<std::mutex> lock(mtx);
    return index.find(id) != index.end();
}

void Storage::remove(int id) {
    std::lock_guard<std::mutex> lock(mtx);
    index.erase(id);
}

void Storage::load_index() {
    std::lock_guard<std::mutex> lock(mtx);
    int sid = 0;
    while (fs::exists(base_name + std::to_string(sid) + ".db")) {
        std::string fn = base_name + std::to_string(sid) + ".db";
        std::ifstream in(fn, std::ios::binary);
        while (in.peek() != EOF) {
            std::streampos p = in.tellg();
            int id; uint32_t len;
            if (!in.read((char*)&id, sizeof(int))) break;
            if (!in.read((char*)&len, sizeof(uint32_t))) break;
            index[id] = { fn, p };
            in.seekg(len, std::ios::cur);
        }
        sid++;
    }
    current_seg_id = std::max(0, sid - 1);
}