#include "storage.h"
#include <iostream>
#include <map>
#include <sstream>
#include <algorithm>
#include <fstream>
#include <cstring> // Для memcpy
#include <zlib.h>



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

std::vector<char> Storage::pack_bools(const std::vector<char>& bool_bytes) {
    std::vector<char> packed;
    if (bool_bytes.empty()) return packed;

    size_t packed_size = (bool_bytes.size() + 7) / 8; // Округляем вверх
    packed.assign(packed_size, 0);

    for (size_t i = 0; i < bool_bytes.size(); ++i) {
        if (bool_bytes[i]) { // Если true (не 0)
            packed[i / 8] |= (1 << (i % 8)); // Устанавливаем конкретный бит
        }
    }
    return packed;
}

std::vector<char> Storage::unpack_bools(const std::vector<char>& packed, size_t original_count) {
    std::vector<char> bool_bytes(original_count, 0);
    for (size_t i = 0; i < original_count; ++i) {
        if (packed[i / 8] & (1 << (i % 8))) {
            bool_bytes[i] = 1;
        }
    }
    return bool_bytes;
}

// Вырезает из записи поле с заданным хешем: убирает его KeyRecord, его байты из
// тела и сдвигает смещения полей, лежавших после него. Значение уходит в value.
// Если поля нет, запись возвращается без изменений, found остаётся false.
std::vector<char> Storage::strip_field(const char* payload, size_t payload_size,
                                       uint32_t field_hash, std::string& value, bool& found) {
    found = false;

    BinaryHeader head;
    memcpy(&head, payload, sizeof(BinaryHeader));

    const size_t keys_offset = sizeof(BinaryHeader);
    const size_t body_offset = keys_offset + head.keys_count * sizeof(KeyRecord);
    const size_t body_size = payload_size - body_offset;

    std::vector<KeyRecord> kept;
    kept.reserve(head.keys_count);

    uint32_t removed_offset = 0;
    uint32_t removed_size = 0;

    for (uint32_t i = 0; i < head.keys_count; ++i) {
        KeyRecord r;
        memcpy(&r, payload + keys_offset + i * sizeof(KeyRecord), sizeof(KeyRecord));

        if (!found && r.key_hash == field_hash) {
            found = true;
            removed_offset = r.data_offset;
            removed_size = r.data_size;
            value.assign(payload + body_offset + r.data_offset, r.data_size);
            continue;
        }
        kept.push_back(r);
    }

    if (!found) return std::vector<char>(payload, payload + payload_size);

    for (auto& r : kept) {
        if (r.data_offset > removed_offset) r.data_offset -= removed_size;
    }

    BinaryHeader new_head = head;
    new_head.keys_count = static_cast<uint32_t>(kept.size());
    new_head.total_size = static_cast<uint32_t>(sizeof(BinaryHeader)
                        + kept.size() * sizeof(KeyRecord)
                        + (body_size - removed_size));

    std::vector<char> out;
    out.reserve(new_head.total_size);
    out.insert(out.end(), (char*)&new_head, (char*)&new_head + sizeof(BinaryHeader));
    for (const auto& r : kept) out.insert(out.end(), (char*)&r, (char*)&r + sizeof(KeyRecord));

    out.insert(out.end(), payload + body_offset, payload + body_offset + removed_offset);
    out.insert(out.end(), payload + body_offset + removed_offset + removed_size,
                          payload + body_offset + body_size);
    return out;
}

// Обратная операция: дописывает значение в конец тела и добавляет его KeyRecord.
// Порядок полей в записи при этом меняется, но поиск идёт по хешу, а не по позиции.
std::vector<char> Storage::insert_field(const char* payload, size_t payload_size,
                                        uint32_t field_hash, const std::string& value) {
    BinaryHeader head;
    memcpy(&head, payload, sizeof(BinaryHeader));

    const size_t keys_offset = sizeof(BinaryHeader);
    const size_t body_offset = keys_offset + head.keys_count * sizeof(KeyRecord);
    const size_t body_size = payload_size - body_offset;

    BinaryHeader new_head = head;
    new_head.keys_count = head.keys_count + 1;
    new_head.total_size = static_cast<uint32_t>(sizeof(BinaryHeader)
                        + new_head.keys_count * sizeof(KeyRecord)
                        + body_size + value.size());

    KeyRecord added;
    added.key_hash = field_hash;
    added.data_offset = static_cast<uint32_t>(body_size);
    added.data_size = static_cast<uint32_t>(value.size());

    std::vector<char> out;
    out.reserve(new_head.total_size);
    out.insert(out.end(), (char*)&new_head, (char*)&new_head + sizeof(BinaryHeader));
    out.insert(out.end(), payload + keys_offset, payload + body_offset);
    out.insert(out.end(), (char*)&added, (char*)&added + sizeof(KeyRecord));
    out.insert(out.end(), payload + body_offset, payload + body_offset + body_size);
    out.insert(out.end(), value.begin(), value.end());
    return out;
}

std::vector<char> Storage::pack_columns(Table* t) {
    const auto& row_buffer = t->write_buffer;
    if (row_buffer.empty()) return {};

    std::vector<uint32_t> sizes;
    std::vector<int> ids;
    std::vector<char> payloads; // Сюда мы будем складывать "остатки" данных
    std::vector<char> bool_column; // Наша новая колонка для BOOL!

    // 1. Ищем, есть ли в схеме колонка типа BOOL
    bool has_bool = false;
    uint32_t bool_hash = 0;
    for (const auto& col : t->schema) {
        if (col.type == DataType::BOOL) {
            has_bool = true;
            bool_hash = hash_string(col.name);
            break; // Пока поддерживаем один BOOL для простоты
        }
    }

    size_t offset = 0;
    while (offset < row_buffer.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, row_buffer.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        int id;
        memcpy(&id, row_buffer.data() + offset, sizeof(int));
        ids.push_back(id);
        
        if (rec_sz > sizeof(int)) {
            size_t payload_size = rec_sz - sizeof(int);
            const char* payload_ptr = row_buffer.data() + offset + sizeof(int);

            if (has_bool) {
                // Значение уходит в битовую колонку и вырезается из самой записи,
                // иначе оно лежало бы в блоке дважды.
                std::string value;
                bool found = false;
                std::vector<char> stripped = strip_field(payload_ptr, payload_size,
                                                         bool_hash, value, found);

                bool_column.push_back(
                    (found && value != "0" && value != "false") ? 1 : 0);

                sizes.push_back(static_cast<uint32_t>(sizeof(int) + stripped.size()));
                payloads.insert(payloads.end(), stripped.begin(), stripped.end());
            } else {
                sizes.push_back(rec_sz);
                payloads.insert(payloads.end(), payload_ptr, payload_ptr + payload_size);
            }

            offset += sizeof(int) + payload_size;
        } else {
            sizes.push_back(rec_sz);
            offset += sizeof(int); // Надгробие
            if (has_bool) bool_column.push_back(0); // Заглушка для удаленной записи
        }
    }

    // --- ЛОГИЧЕСКОЕ СЖАТИЕ ---
    
    // 1. Delta-кодирование для ID
    std::vector<int> delta_ids;
    if (!ids.empty()) {
        delta_ids.reserve(ids.size());
        delta_ids.push_back(ids[0]);
        for (size_t i = 1; i < ids.size(); ++i) {
            delta_ids.push_back(ids[i] - ids[i - 1]);
        }
    }

    // 2. Битовая упаковка для BOOL (Сжатие 8х!)
    std::vector<char> packed_bools;
    if (has_bool) {
        packed_bools = pack_bools(bool_column);
    }

    // --- СКЛЕЙКА В ФИНАЛЬНЫЙ БЛОК ---
    std::vector<char> columnar_buffer;
    uint32_t count = sizes.size();
    
    columnar_buffer.insert(columnar_buffer.end(), (char*)&count, (char*)&count + sizeof(uint32_t));
    columnar_buffer.insert(columnar_buffer.end(), (char*)sizes.data(), (char*)(sizes.data() + sizes.size()));
    columnar_buffer.insert(columnar_buffer.end(), (char*)delta_ids.data(), (char*)(delta_ids.data() + delta_ids.size()));
    
    // Пишем информацию о BOOL колонке
    char bool_flag = has_bool ? 1 : 0;
    columnar_buffer.insert(columnar_buffer.end(), &bool_flag, &bool_flag + 1);
    if (has_bool) {
        uint32_t packed_bool_size = packed_bools.size();
        columnar_buffer.insert(columnar_buffer.end(), (char*)&packed_bool_size, (char*)&packed_bool_size + sizeof(uint32_t));
        columnar_buffer.insert(columnar_buffer.end(), packed_bools.begin(), packed_bools.end());
    }

    columnar_buffer.insert(columnar_buffer.end(), payloads.begin(), payloads.end());

    return columnar_buffer;
}

std::vector<char> Storage::unpack_columns(const std::vector<char>& columnar_buffer, Table* t) {
    if (columnar_buffer.empty()) return {};

    std::vector<char> row_buffer;
    
    uint32_t count;
    size_t col_offset = 0;
    memcpy(&count, columnar_buffer.data() + col_offset, sizeof(uint32_t));
    col_offset += sizeof(uint32_t);

    const uint32_t* sizes_ptr = reinterpret_cast<const uint32_t*>(columnar_buffer.data() + col_offset);
    col_offset += count * sizeof(uint32_t);

    const int* delta_ids_ptr = reinterpret_cast<const int*>(columnar_buffer.data() + col_offset);
    col_offset += count * sizeof(int);

    // ==========================================
    // 1. ВОССТАНОВЛЕНИЕ ID ИЗ ДЕЛЬТ
    // ==========================================
    std::vector<int> restored_ids(count);
    if (count > 0) {
        restored_ids[0] = delta_ids_ptr[0];
        for (uint32_t i = 1; i < count; ++i) {
            restored_ids[i] = restored_ids[i - 1] + delta_ids_ptr[i]; 
        }
    }

    // ==========================================
    // 2. РАСПАКОВКА BOOL-КОЛОНКИ
    // ==========================================
    char bool_flag = columnar_buffer[col_offset];
    col_offset += 1;
    
    std::vector<char> unpacked_bools;
    if (bool_flag == 1) {
        uint32_t packed_bool_size;
        memcpy(&packed_bool_size, columnar_buffer.data() + col_offset, sizeof(uint32_t));
        col_offset += sizeof(uint32_t);
        
        std::vector<char> packed_bools(columnar_buffer.data() + col_offset, columnar_buffer.data() + col_offset + packed_bool_size);
        col_offset += packed_bool_size;
        
        // Разжимаем 1 байт обратно в 8 отдельных bool-значений
        unpacked_bools = unpack_bools(packed_bools, count);
    }

    // 3. Чтение оставшихся данных (payloads)
    const char* payloads_ptr = columnar_buffer.data() + col_offset;
    size_t payload_offset = 0;

    // Хеш bool-колонки берём из схемы: в блоке он не хранится.
    // Поэтому переименование bool-колонки делает старые блоки нечитаемыми —
    // эволюция схемы не поддерживается.
    uint32_t bool_hash = 0;
    for (const auto& col : t->schema) {
        if (col.type == DataType::BOOL) {
            bool_hash = hash_string(col.name);
            break;
        }
    }

    // ==========================================
    // 4. СБОРКА СТРОК (Row-based format)
    // ==========================================
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t stored_sz = sizes_ptr[i];
        int id = restored_ids[i];

        if (stored_sz > sizeof(int)) {
            size_t payload_size = stored_sz - sizeof(int);
            const char* payload_ptr = payloads_ptr + payload_offset;

            std::vector<char> restored;
            if (bool_flag == 1 && bool_hash != 0 && i < unpacked_bools.size()) {
                // Возвращаем в запись значение, вырезанное при упаковке.
                std::string value = unpacked_bools[i] ? "true" : "false";
                restored = insert_field(payload_ptr, payload_size, bool_hash, value);
            } else {
                restored.assign(payload_ptr, payload_ptr + payload_size);
            }

            uint32_t rec_sz = static_cast<uint32_t>(sizeof(int) + restored.size());
            row_buffer.insert(row_buffer.end(), (char*)&rec_sz, (char*)&rec_sz + sizeof(uint32_t));
            row_buffer.insert(row_buffer.end(), (char*)&id, (char*)&id + sizeof(int));
            row_buffer.insert(row_buffer.end(), restored.begin(), restored.end());

            payload_offset += payload_size;
        } else {
            row_buffer.insert(row_buffer.end(), (char*)&stored_sz, (char*)&stored_sz + sizeof(uint32_t));
            row_buffer.insert(row_buffer.end(), (char*)&id, (char*)&id + sizeof(int));
        }
    }

    return row_buffer;
}

// Читает один сжатый блок из текущей позиции потока и возвращает его
// в строковом (row-based) виде: zlib -> колонки -> строки.
// Единственное место, где блок распаковывается, поэтому кодек записи и
// кодек чтения не могут разойтись.
// Возвращает false, если блок не дочитался или не распаковался.
bool Storage::read_block(std::ifstream& in, Table* t, std::vector<char>& row_out) {
    CompressedBlockHeader header;
    if (!in.read(reinterpret_cast<char*>(&header), sizeof(header))) return false;

    std::vector<char> comp(header.compressed_size);
    if (!in.read(comp.data(), header.compressed_size)) return false;

    std::vector<char> columnar(header.original_size);
    uLongf dest_len = header.original_size;
    int res = uncompress(
        reinterpret_cast<Bytef*>(columnar.data()),
        &dest_len,
        reinterpret_cast<const Bytef*>(comp.data()),
        header.compressed_size
    );

    if (res != Z_OK || dest_len != header.original_size) return false;

    row_out = unpack_columns(columnar, t);
    return true;
}

// Достаёт значение одного поля, не разбирая запись целиком: ищет хеш имени
// в таблице ключей и читает ровно нужные байты по смещению.
bool Storage::extract_field(const char* data_ptr, const std::string& key, std::string& out) {
    BinaryHeader head;
    memcpy(&head, data_ptr, sizeof(BinaryHeader));

    const uint32_t h = hash_string(key);
    const size_t keys_offset = sizeof(BinaryHeader);
    const size_t body_offset = keys_offset + head.keys_count * sizeof(KeyRecord);

    for (uint32_t i = 0; i < head.keys_count; ++i) {
        KeyRecord r;
        memcpy(&r, data_ptr + keys_offset + i * sizeof(KeyRecord), sizeof(KeyRecord));
        if (r.key_hash == h) {
            out.assign(data_ptr + body_offset + r.data_offset, r.data_size);
            return true;
        }
    }
    return false;
}

// Собирает JSON записи из значений полей по схеме таблицы.
// Раньше для этого в каждой записи лежала копия исходного JSON под ключом
// __full__, то есть все значения хранились на диске дважды.
// Пустая строка означает, что у таблицы нет схемы: без неё имена полей
// восстановить нельзя, в записи лежат только их хеши.
std::string Storage::rebuild_json(const char* data_ptr, Table* t) {
    if (t->schema.empty()) return "";

    std::string out = "{";
    bool first = true;

    for (const auto& col : t->schema) {
        std::string val;
        if (!extract_field(data_ptr, col.name, val)) continue;

        if (!first) out += ",";
        first = false;

        out += "\"" + col.name + "\":";

        if (col.type == DataType::STRING || col.type == DataType::DATE) {
            out += "\"" + val + "\"";
        } else if (col.type == DataType::BOOL) {
            out += (val == "true" || val == "1") ? "true" : "false";
        } else {
            out += val;
        }
    }

    out += "}";
    return out;
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

    // 1. УМНАЯ ПРОВЕРКА ЛИМИТА ДО ОТКРЫТИЯ ФАЙЛА
    std::string path = t->path + "seg_" + std::to_string(t->current_seg_id) + ".db";
    
    // Если текущий файл уже существует и его размер перевалил за MAX_SEG_SIZE
    if (fs::exists(path) && fs::file_size(path) >= MAX_SEG_SIZE) {
        t->current_seg_id++; // Увеличиваем номер сегмента
        path = t->path + "seg_" + std::to_string(t->current_seg_id) + ".db"; // Обновляем путь!
    }

    // 2. ОТКРЫВАЕМ ФАЙЛ
    std::ofstream file(path, std::ios::binary | std::ios::app);
    file.seekp(0, std::ios::end);
    std::streampos block_start = file.tellp(); // Запоминаем позицию для индекса

    // ==========================================
    // НОВЫЙ ШАГ: КОЛОНОЧНАЯ ПЕРЕПАКОВКА
    // ==========================================
    std::vector<char> columnar_data = pack_columns(t);

    // 3. СЖАТИЕ ЧЕРЕЗ ZLIB (DEFLATE)
    uLongf original_size = columnar_data.size();
    uLongf max_compressed_size = compressBound(original_size);
    std::vector<char> compressed(max_compressed_size);

    int res = compress(
        (Bytef*)compressed.data(), 
        &max_compressed_size, 
        (const Bytef*)columnar_data.data(), // ЖМЕМ УЖЕ КОЛОНКИ!
        original_size
    );

    if (res != Z_OK) {
        // Ошибка сжатия (в идеале нужно залогировать)
        file.close();
        return; 
    }

    CompressedBlockHeader header;
    header.original_size = original_size;
    header.compressed_size = max_compressed_size; // Пишем реальный размер

    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.write(compressed.data(), max_compressed_size); // Пишем ровно столько, сколько сжалось

    // 4. ОБНОВЛЕНИЕ ИНДЕКСА
    size_t offset = 0;
    while (offset < t->write_buffer.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, t->write_buffer.data() + offset, sizeof(uint32_t));
        int id;
        memcpy(&id, t->write_buffer.data() + offset + sizeof(uint32_t), sizeof(int));
        
        if (rec_sz == sizeof(int)) {
            t->index.erase(id); // Удаляем из индекса, если надгробие
        } else {
            // Пишем актуальный путь (path) и смещение
            t->index[id] = { path, static_cast<size_t>(block_start) };
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
        if (!target_key.empty()) {
            std::string val;
            if (!extract_field(data_ptr, target_key, val)) return "ERR_KEY_NOT_FOUND";
            return val;
        }
        std::string json = rebuild_json(data_ptr, t);
        return json.empty() ? "ERR_NO_SCHEMA" : json;
    };

    // 1. ИЩЕМ В WRITE_BUFFER (Ждем до конца, берем самое свежее)
    // В буфере данные лежат в обычном строковом (Row-based) виде, поэтому читаем напрямую!
    bool found_in_buffer = false;
    std::string latest_buffer_result = "ERR_NOT_FOUND";
    size_t buf_off = 0;
    
    while (buf_off < t->write_buffer.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, t->write_buffer.data() + buf_off, sizeof(uint32_t));
        int rid;
        memcpy(&rid, t->write_buffer.data() + buf_off + sizeof(uint32_t), sizeof(int));
        
        if (rid == id) {
            found_in_buffer = true;
            if (rec_sz == sizeof(int)) {
                latest_buffer_result = "ERR_NOT_FOUND"; // Нашли надгробие!
            } else {
                latest_buffer_result = extract(t->write_buffer.data() + buf_off + sizeof(uint32_t) + sizeof(int));
            }
            // НЕ ДЕЛАЕМ return. Продолжаем искать более свежие версии!
        }
        buf_off += sizeof(uint32_t) + rec_sz;
    }

    if (found_in_buffer) {
        return latest_buffer_result;
    }

    // 2. ИЩЕМ НА ДИСКЕ В ИНДЕКСЕ
    if (t->index.find(id) == t->index.end()) return "ERR_NOT_FOUND";
    FileLocation loc = t->index[id];

    std::ifstream in(loc.filename, std::ios::binary);
    in.seekg(loc.offset);

    CompressedBlockHeader header;
    in.read((char*)&header, sizeof(header));
    std::vector<char> comp(header.compressed_size);
    in.read(comp.data(), header.compressed_size);

    std::vector<char> orig(header.original_size);
    
    // ==========================================
    // РАСПАКОВКА ЧЕРЕЗ ZLIB (DEFLATE)
    // ==========================================
    uLongf dest_len = header.original_size;
    int uncomp_res = uncompress(
        (Bytef*)orig.data(), 
        &dest_len, 
        (const Bytef*)comp.data(), 
        header.compressed_size
    );

    if (uncomp_res != Z_OK) {
        return "ERR_ZLIB_DECOMPRESSION_FAILED";
    }

    // ==========================================
    // МАГИЯ: РАСПАКОВКА КОЛОНОК ОБРАТНО В СТРОКИ
    // ==========================================
    // Массив orig сейчас хранит колонки. Превращаем их обратно в удобные строки.
    std::vector<char> row_orig = unpack_columns(orig, t);

    // 3. ИЩЕМ В РАСПАКОВАННОМ БЛОКЕ (Используем row_orig вместо orig)
    bool found_in_block = false;
    std::string latest_block_result = "ERR_NOT_FOUND";
    size_t offset = 0;
    
    while (offset < row_orig.size()) {
        uint32_t rec_sz;
        memcpy(&rec_sz, row_orig.data() + offset, sizeof(uint32_t));
        int rid;
        memcpy(&rid, row_orig.data() + offset + sizeof(uint32_t), sizeof(int));

        if (rid == id) {
            found_in_block = true;
            if (rec_sz == sizeof(int)) {
                latest_block_result = "ERR_NOT_FOUND"; // Нашли надгробие в блоке!
            } else {
                latest_block_result = extract(row_orig.data() + offset + sizeof(uint32_t) + sizeof(int));
            }
        }
        offset += sizeof(uint32_t) + rec_sz;
    }
    
    if (found_in_block) {
        return latest_block_result;
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
        std::string json = rebuild_json(data_ptr, t);
        return json.empty() ? "{}" : json;
    };

    int sid = 0;
    while (fs::exists(t->path + "seg_" + std::to_string(sid) + ".db")) {
        std::ifstream in(t->path + "seg_" + std::to_string(sid) + ".db", std::ios::binary);
        while (in.peek() != EOF && in.good()) {
            std::vector<char> rows;
            if (!read_block(in, t, rows)) break;

            size_t offset = 0;
            while (offset < rows.size()) {
                uint32_t rec_sz;
                memcpy(&rec_sz, rows.data() + offset, sizeof(uint32_t));
                int id;
                memcpy(&id, rows.data() + offset + sizeof(uint32_t), sizeof(int));
                
                if (rec_sz == sizeof(int)) {
                    latest_data.erase(id);
                } else {
                    latest_data[id] = extract(rows.data() + offset + sizeof(uint32_t) + sizeof(int));
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

            std::vector<char> rows;
            if (!read_block(in, t, rows)) break;
            
            size_t offset = 0;
            while (offset < rows.size()) {
                uint32_t rec_sz;
                memcpy(&rec_sz, rows.data() + offset, sizeof(uint32_t));
                int id;
                memcpy(&id, rows.data() + offset + sizeof(uint32_t), sizeof(int));
                
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

