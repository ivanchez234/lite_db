#include "storage.h"
#include "byte_reader.h"
#include "file_sync.h"

#include <shared_mutex>
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

namespace {

// Одна запись в строковом буфере: [uint32 длина][int id][payload].
struct RecordView {
    uint32_t    rec_sz       = 0;
    int         id           = 0;
    const char* payload      = nullptr;  // nullptr у надгробия
    std::size_t payload_size = 0;
};

// Читает запись по смещению offset и сдвигает его на следующую.
// false означает конец буфера либо повреждённые данные: длина записи взята
// из самого буфера, и доверять ей без проверки нельзя.
bool next_record(const char* data, std::size_t size, std::size_t& offset, RecordView& out) {
    if (offset > size) return false;

    ByteReader r(data + offset, size - offset);

    uint32_t rec_sz = 0;
    if (!r.read(rec_sz)) return false;
    if (rec_sz < sizeof(int)) return false;   // не вмещает даже id
    if (!r.has(rec_sz)) return false;         // запись обрезана

    int id = 0;
    r.read(id);

    out.rec_sz       = rec_sz;
    out.id           = id;
    out.payload_size = rec_sz - sizeof(int);
    out.payload      = out.payload_size ? data + offset + sizeof(uint32_t) + sizeof(int)
                                        : nullptr;

    offset += sizeof(uint32_t) + rec_sz;
    return true;
}

} // namespace

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

    std::vector<char> unchanged(payload, payload + payload_size);

    ByteReader reader(payload, payload_size);

    BinaryHeader head;
    if (!reader.read(head)) return unchanged;

    const size_t keys_offset = sizeof(BinaryHeader);
    if (head.keys_count > (payload_size - keys_offset) / sizeof(KeyRecord)) return unchanged;

    const size_t body_offset = keys_offset + head.keys_count * sizeof(KeyRecord);
    const size_t body_size = payload_size - body_offset;

    std::vector<KeyRecord> kept;
    kept.reserve(head.keys_count);

    uint32_t removed_offset = 0;
    uint32_t removed_size = 0;

    for (uint32_t i = 0; i < head.keys_count; ++i) {
        KeyRecord r;
        if (!reader.read(r)) return unchanged;
        if (r.data_offset > body_size || body_size - r.data_offset < r.data_size) return unchanged;

        if (!found && r.key_hash == field_hash) {
            found = true;
            removed_offset = r.data_offset;
            removed_size = r.data_size;
            value.assign(payload + body_offset + r.data_offset, r.data_size);
            continue;
        }
        kept.push_back(r);
    }

    if (!found) return unchanged;

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
    if (payload_size < sizeof(BinaryHeader)) {
        return std::vector<char>(payload, payload + payload_size);
    }

    BinaryHeader head;
    memcpy(&head, payload, sizeof(BinaryHeader));

    const size_t keys_offset = sizeof(BinaryHeader);
    if (head.keys_count > (payload_size - keys_offset) / sizeof(KeyRecord)) {
        return std::vector<char>(payload, payload + payload_size);
    }

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
    RecordView rec;
    while (next_record(row_buffer.data(), row_buffer.size(), offset, rec)) {
        ids.push_back(rec.id);

        if (rec.payload_size > 0) {
            if (has_bool) {
                // Значение уходит в битовую колонку и вырезается из самой записи,
                // иначе оно лежало бы в блоке дважды.
                std::string value;
                bool found = false;
                std::vector<char> stripped = strip_field(rec.payload, rec.payload_size,
                                                         bool_hash, value, found);

                bool_column.push_back(
                    (found && value != "0" && value != "false") ? 1 : 0);

                sizes.push_back(static_cast<uint32_t>(sizeof(int) + stripped.size()));
                payloads.insert(payloads.end(), stripped.begin(), stripped.end());
            } else {
                sizes.push_back(rec.rec_sz);
                payloads.insert(payloads.end(), rec.payload, rec.payload + rec.payload_size);
            }
        } else {
            sizes.push_back(rec.rec_sz);   // Надгробие
            if (has_bool) bool_column.push_back(0);
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

    ByteReader reader(columnar_buffer.data(), columnar_buffer.size());

    uint32_t count = 0;
    if (!reader.read(count)) return {};

    // count прочитан из блока. Сначала убеждаемся, что столько колонок вообще
    // помещается в оставшиеся байты, и только потом умножаем на размер элемента.
    if (count > reader.remaining() / (sizeof(uint32_t) + sizeof(int))) return {};

    const char* sizes_raw = reader.take(count * sizeof(uint32_t));
    if (!sizes_raw) return {};
    const char* deltas_raw = reader.take(count * sizeof(int));
    if (!deltas_raw) return {};

    // Копируем, а не читаем через reinterpret_cast: смещение в блоке
    // произвольное, и приведение указателя дало бы невыровненный доступ.
    std::vector<uint32_t> sizes(count);
    std::vector<int> deltas(count);
    if (count > 0) {
        memcpy(sizes.data(), sizes_raw, count * sizeof(uint32_t));
        memcpy(deltas.data(), deltas_raw, count * sizeof(int));
    }

    // ==========================================
    // 1. ВОССТАНОВЛЕНИЕ ID ИЗ ДЕЛЬТ
    // ==========================================
    std::vector<int> restored_ids(count);
    if (count > 0) {
        restored_ids[0] = deltas[0];
        for (uint32_t i = 1; i < count; ++i) {
            restored_ids[i] = restored_ids[i - 1] + deltas[i];
        }
    }

    // ==========================================
    // 2. РАСПАКОВКА BOOL-КОЛОНКИ
    // ==========================================
    char bool_flag = 0;
    if (!reader.read(bool_flag)) return {};

    std::vector<char> unpacked_bools;
    if (bool_flag == 1) {
        uint32_t packed_bool_size = 0;
        if (!reader.read(packed_bool_size)) return {};

        const char* packed_raw = reader.take(packed_bool_size);
        if (!packed_raw) return {};

        // Битовая колонка обязана покрывать все записи блока.
        if (packed_bool_size < (count + 7) / 8) return {};

        unpacked_bools = unpack_bools(
            std::vector<char>(packed_raw, packed_raw + packed_bool_size), count);
    }

    // 3. Чтение оставшихся данных (payloads)
    const char* payloads_ptr = columnar_buffer.data() + reader.offset();
    const size_t payloads_size = reader.remaining();
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
        uint32_t stored_sz = sizes[i];
        int id = restored_ids[i];

        if (stored_sz > sizeof(int)) {
            size_t payload_size = stored_sz - sizeof(int);

            // Длина записи взята из блока: она должна укладываться в то,
            // что от блока осталось.
            if (payloads_size - payload_offset < payload_size) break;

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

    // Размеры пришли из файла: проверяем до того, как выделять по ним память.
    if (header.compressed_size == 0 || header.original_size == 0) return false;
    if (header.compressed_size > MAX_BLOCK_SIZE || header.original_size > MAX_BLOCK_SIZE) return false;

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
bool Storage::extract_field(const char* data_ptr, size_t payload_size,
                            const std::string& key, std::string& out) {
    ByteReader reader(data_ptr, payload_size);

    BinaryHeader head;
    if (!reader.read(head)) return false;

    // keys_count прочитан из записи: он не должен описывать больше ключей,
    // чем помещается в оставшиеся байты.
    const size_t keys_offset = sizeof(BinaryHeader);
    if (head.keys_count > (payload_size - keys_offset) / sizeof(KeyRecord)) return false;

    const size_t body_offset = keys_offset + head.keys_count * sizeof(KeyRecord);
    const size_t body_size = payload_size - body_offset;

    const uint32_t h = hash_string(key);

    for (uint32_t i = 0; i < head.keys_count; ++i) {
        KeyRecord r;
        if (!reader.read(r)) return false;
        if (r.key_hash != h) continue;

        // Смещение и длина значения тоже из записи.
        if (r.data_offset > body_size) return false;
        if (body_size - r.data_offset < r.data_size) return false;

        out.assign(data_ptr + body_offset + r.data_offset, r.data_size);
        return true;
    }
    return false;
}

// Собирает JSON записи из значений полей по схеме таблицы.
// Раньше для этого в каждой записи лежала копия исходного JSON под ключом
// __full__, то есть все значения хранились на диске дважды.
// Пустая строка означает, что у таблицы нет схемы: без неё имена полей
// восстановить нельзя, в записи лежат только их хеши.
std::string Storage::rebuild_json(const char* data_ptr, size_t payload_size, Table* t) {
    if (t->schema.empty()) return "";

    std::string out = "{";
    bool first = true;

    for (const auto& col : t->schema) {
        std::string val;
        if (!extract_field(data_ptr, payload_size, col.name, val)) continue;

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
        // Неудачный сброс не теряет данных: они остаются в буфере (и в журнале),
        // а следующая запись попробует сбросить их снова.
        if (!flush_block_to_disk(t)) {
            std::cerr << "[Storage] Could not flush block of table '" << t->name
                      << "', keeping it in memory" << std::endl;
        }
    }
}

bool Storage::flush_block_to_disk(Table* t) {
    if (t->write_buffer.empty()) return true;

    // 1. УМНАЯ ПРОВЕРКА ЛИМИТА ДО ОТКРЫТИЯ ФАЙЛА
    std::string path = t->path + "seg_" + std::to_string(t->current_seg_id) + ".db";
    
    // Если текущий файл уже существует и его размер перевалил за MAX_SEG_SIZE
    std::error_code size_ec;
    if (fs::exists(path) && fs::file_size(path, size_ec) >= MAX_SEG_SIZE && !size_ec) {
        t->current_seg_id++; // Увеличиваем номер сегмента
        path = t->path + "seg_" + std::to_string(t->current_seg_id) + ".db"; // Обновляем путь!
    }

    // ==========================================
    // КОЛОНОЧНАЯ ПЕРЕПАКОВКА И СЖАТИЕ — ДО ОТКРЫТИЯ ФАЙЛА
    // ==========================================
    // Если сжатие не удастся, файл не трогаем вовсе.
    std::vector<char> columnar_data = pack_columns(t);

    uLongf original_size = columnar_data.size();
    uLongf compressed_size = compressBound(original_size);
    std::vector<char> compressed(compressed_size);

    int res = compress(
        (Bytef*)compressed.data(), 
        &compressed_size, 
        (const Bytef*)columnar_data.data(), // ЖМЕМ УЖЕ КОЛОНКИ!
        original_size
    );
    if (res != Z_OK) return false;

    CompressedBlockHeader header;
    header.original_size = static_cast<uint32_t>(original_size);
    header.compressed_size = static_cast<uint32_t>(compressed_size); // Пишем реальный размер

    // ==========================================
    // ЗАПИСЬ БЛОКА
    // ==========================================
    std::ofstream file(path, std::ios::binary | std::ios::app);
    if (!file.is_open()) return false;
    file.seekp(0, std::ios::end);
    const std::streampos block_start = file.tellp(); // Запоминаем позицию для индекса

    file.write(reinterpret_cast<const char*>(&header), sizeof(header));
    file.write(compressed.data(), static_cast<std::streamsize>(compressed_size));
    file.flush();
    const bool written = static_cast<bool>(file);
    file.close();

    if (!written || file.fail()) {
        // Блок мог записаться наполовину (например, кончилось место). Обрезаем
        // хвост, иначе следующий блок встанет после мусора и при перезапуске
        // чтение сегмента остановится на этом мусоре вместе со всем, что за ним.
        std::error_code ec;
        fs::resize_file(path, static_cast<std::uintmax_t>(block_start), ec);
        return false;
    }

    // ==========================================
    // ОБНОВЛЕНИЕ ИНДЕКСА — ТОЛЬКО ПОСЛЕ УСПЕШНОЙ ЗАПИСИ
    // ==========================================
    size_t offset = 0;
    RecordView rec;
    while (next_record(t->write_buffer.data(), t->write_buffer.size(), offset, rec)) {
        if (rec.payload_size == 0) {
            t->index.erase(rec.id); // Удаляем из индекса, если надгробие
        } else {
            // Пишем актуальный путь (path) и смещение
            t->index[rec.id] = { path, block_start };
        }
    }

    t->unsynced_segments.insert(path);
    t->write_buffer.clear(); 
    return true;
}

bool Storage::sync_segments(Table* t) {
    for (auto it = t->unsynced_segments.begin(); it != t->unsynced_segments.end();) {
        if (!lite_db::sync_file(*it)) return false;
        it = t->unsynced_segments.erase(it);
    }
    // Запись о самих файлах сегментов живёт в каталоге таблицы.
    return lite_db::sync_directory(t->path);
}

bool Storage::flush_all() {
    std::shared_lock<std::shared_mutex> catalog(tables_mtx);

    bool ok = true;
    for (auto& entry : tables) {
        Table* t = entry.second.get();
        std::unique_lock<std::shared_mutex> lock(t->mtx);

        if (!flush_block_to_disk(t)) {
            std::cerr << "[Storage] FLUSH: could not write table '" << t->name << "'" << std::endl;
            ok = false;
        } else if (!sync_segments(t)) {
            std::cerr << "[Storage] FLUSH: fsync failed for table '" << t->name << "'" << std::endl;
            ok = false;
        }
    }

    // Каталоги таблиц создаются внутри root_path.
    if (!lite_db::sync_directory(root_path)) ok = false;
    return ok;
}

Table* Storage::find_table(const std::string& name) {
    const auto it = tables.find(name);
    return it == tables.end() ? nullptr : it->second.get();
}

Table* Storage::lookup_table(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(tables_mtx);
    return find_table(name);
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
            k.erase(0, k.find_first_not_of(' ')); k.erase(k.find_last_not_of(' ') + 1);
            v.erase(0, v.find_first_not_of(' ')); v.erase(v.find_last_not_of(' ') + 1);
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

bool Storage::pack_json(const std::string& json_str, Table* t, std::vector<char>& buf) {
    auto data = parse_json_manual(json_str);
    // Несоответствие схеме — обычный ответ клиенту, а не авария, поэтому код возврата.
    if (!validate_types(t, data)) return false;

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

    buf.clear();
    buf.insert(buf.end(), (char*)&head, (char*)&head + sizeof(BinaryHeader));
    for (auto& r : records) buf.insert(buf.end(), (char*)&r, (char*)&r + sizeof(KeyRecord));
    buf.insert(buf.end(), body.begin(), body.end());
    return true;
}

bool Storage::create_table(const std::string& name) {
    {
        std::shared_lock<std::shared_mutex> lock(tables_mtx);
        if (find_table(name)) return false;
    }

    // Таблицу собираем без замка каталога: загрузка индекса читает все
    // сегменты с диска, и держать на это время замок значило бы остановить
    // все запросы ко всем таблицам.
    auto t = std::make_unique<Table>();
    t->name = name;
    t->path = root_path + name + "/";

    std::error_code ec;
    fs::create_directories(t->path, ec);
    if (ec) return false;

    load_schema(t.get());
    load_table_index(t.get());

    std::unique_lock<std::shared_mutex> lock(tables_mtx);
    // Пока мы читали диск, ту же таблицу мог создать другой поток.
    return tables.emplace(name, std::move(t)).second;
}

bool Storage::set_schema(const std::string& table_name, const std::vector<Column>& columns) {
    Table* t = lookup_table(table_name);
    if (!t) return false;

    // Схему читают запросы к таблице, поэтому меняем её под замком таблицы.
    std::unique_lock<std::shared_mutex> lock(t->mtx);
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

WriteResult Storage::insert(const std::string& table_name, int id, const std::string& json_str,
                           WriteMode mode, const WriteLog& log) {
    Table* t = lookup_table(table_name);
    if (!t) return WriteResult::table_not_found;

    std::unique_lock<std::shared_mutex> t_lock(t->mtx);

    // Проверка и вставка под одним замком. Если проверить отдельно, два
    // INSERT с одним id могут оба увидеть «свободно» и оба записаться.
    if (mode == WriteMode::insert && exists_locked(t, id)) return WriteResult::id_exists;

    // Сначала проверяем данные, потом пишем в журнал: мусор, который всё
    // равно будет отвергнут, не должен попадать в журнал.
    std::vector<char> bin;
    if (!pack_json(json_str, t, bin)) return WriteResult::invalid_data;

    // Журнал под замком таблицы: порядок записей в журнале совпадает
    // с порядком применения, и восстановление воспроизведёт ту же историю.
    if (log && !log()) return WriteResult::log_failed;

    std::vector<char> full_record;
    full_record.insert(full_record.end(), reinterpret_cast<char*>(&id), reinterpret_cast<char*>(&id) + sizeof(int));
    full_record.insert(full_record.end(), bin.begin(), bin.end());

    insert_to_block(t, full_record);
    return WriteResult::ok;
}

std::string Storage::select(const std::string& table_name, int id, const std::string& target_key) {
    Table* t = nullptr;
    {
        std::shared_lock<std::shared_mutex> lock(tables_mtx);
        t = find_table(table_name);
        if (!t) return "ERR_TABLE_NOT_FOUND";
    }
    // Чтение: несколько запросов к одной таблице идут одновременно.
    std::shared_lock<std::shared_mutex> t_lock(t->mtx);
    
    auto extract = [&](const char* data_ptr, size_t payload_size) -> std::string {
        if (!target_key.empty()) {
            std::string val;
            if (!extract_field(data_ptr, payload_size, target_key, val)) return "ERR_KEY_NOT_FOUND";
            return val;
        }
        std::string json = rebuild_json(data_ptr, payload_size, t);
        return json.empty() ? "ERR_NO_SCHEMA" : json;
    };

    // 1. ИЩЕМ В WRITE_BUFFER (Ждем до конца, берем самое свежее)
    // В буфере данные лежат в обычном строковом (Row-based) виде, поэтому читаем напрямую!
    bool found_in_buffer = false;
    std::string latest_buffer_result = "ERR_NOT_FOUND";
    size_t buf_off = 0;
    RecordView rec;

    while (next_record(t->write_buffer.data(), t->write_buffer.size(), buf_off, rec)) {
        if (rec.id == id) {
            found_in_buffer = true;
            if (rec.payload_size == 0) {
                latest_buffer_result = "ERR_NOT_FOUND"; // Нашли надгробие!
            } else {
                latest_buffer_result = extract(rec.payload, rec.payload_size);
            }
            // НЕ ДЕЛАЕМ return. Продолжаем искать более свежие версии!
        }
    }

    if (found_in_buffer) {
        return latest_buffer_result;
    }

    // 2. ИЩЕМ НА ДИСКЕ В ИНДЕКСЕ
    // Именно find, а не operator[]: тот не const и под разделяемым замком
    // стал бы изменением карты из нескольких потоков сразу.
    const auto index_it = t->index.find(id);
    if (index_it == t->index.end()) return "ERR_NOT_FOUND";
    const FileLocation loc = index_it->second;

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
    RecordView block_rec;

    while (next_record(row_orig.data(), row_orig.size(), offset, block_rec)) {
        if (block_rec.id == id) {
            found_in_block = true;
            if (block_rec.payload_size == 0) {
                latest_block_result = "ERR_NOT_FOUND"; // Нашли надгробие в блоке!
            } else {
                latest_block_result = extract(block_rec.payload, block_rec.payload_size);
            }
        }
    }
    
    if (found_in_block) {
        return latest_block_result;
    }

    return "ERR_NOT_FOUND";
}
std::string Storage::select_all(const std::string& table_name) {
    Table* t = nullptr;
    {
        std::shared_lock<std::shared_mutex> lock(tables_mtx);
        t = find_table(table_name);
        if (!t) return "ERR_TABLE_NOT_FOUND";
    }
    std::shared_lock<std::shared_mutex> t_lock(t->mtx);
    
    std::map<int, std::string> latest_data;

    auto extract = [&](const char* data_ptr, size_t payload_size) -> std::string {
        std::string json = rebuild_json(data_ptr, payload_size, t);
        return json.empty() ? "{}" : json;
    };

    int sid = 0;
    while (fs::exists(t->path + "seg_" + std::to_string(sid) + ".db")) {
        std::ifstream in(t->path + "seg_" + std::to_string(sid) + ".db", std::ios::binary);
        while (in.peek() != EOF && in.good()) {
            std::vector<char> rows;
            if (!read_block(in, t, rows)) break;

            size_t offset = 0;
            RecordView rec;
            while (next_record(rows.data(), rows.size(), offset, rec)) {
                if (rec.payload_size == 0) {
                    latest_data.erase(rec.id);
                } else {
                    latest_data[rec.id] = extract(rec.payload, rec.payload_size);
                }
            }
        }
        sid++;
    }

    size_t buf_off = 0;
    RecordView buf_rec;
    while (next_record(t->write_buffer.data(), t->write_buffer.size(), buf_off, buf_rec)) {
        if (buf_rec.payload_size == 0) {
            latest_data.erase(buf_rec.id);
        } else {
            latest_data[buf_rec.id] = extract(buf_rec.payload, buf_rec.payload_size);
        }
    }

    if (latest_data.empty()) return "[]";

    // Без переводов строк: в протоколе перевод строки — конец ответа, и
    // многострочный ответ клиент принял бы за несколько разных ответов.
    std::string result = "[";
    bool first = true;
    for (const auto& [id, json_str] : latest_data) {
        if (!first) result += ", ";
        result += "{ \"id\": " + std::to_string(id) + ", \"data\": " + json_str + " }";
        first = false;
    }
    result += "]";
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
            RecordView rec;
            while (next_record(rows.data(), rows.size(), offset, rec)) {
                if (rec.payload_size == 0) {
                    t->index.erase(rec.id);
                } else {
                    t->index[rec.id] = { fn, block_start };
                }
            }
        }
        sid++;
    }
    t->current_seg_id = std::max(0, sid - 1);
}

bool Storage::exists_locked(Table* t, int id) {
    // Буфер новее диска: последняя версия записи в буфере решает всё.
    size_t buf_off = 0;
    bool found_in_buf = false;
    bool is_deleted = false;
    RecordView rec;

    while (next_record(t->write_buffer.data(), t->write_buffer.size(), buf_off, rec)) {
        if (rec.id == id) {
            found_in_buf = true;
            is_deleted = (rec.payload_size == 0);
        }
    }

    if (found_in_buf) return !is_deleted;
    return t->index.find(id) != t->index.end();
}

bool Storage::exists(const std::string& table_name, int id) {
    Table* t = lookup_table(table_name);
    if (!t) return false;
    std::shared_lock<std::shared_mutex> t_lock(t->mtx);
    return exists_locked(t, id);
}

WriteResult Storage::remove(const std::string& table_name, int id, const WriteLog& log) {
    Table* t = lookup_table(table_name);
    if (!t) return WriteResult::table_not_found;
    std::unique_lock<std::shared_mutex> t_lock(t->mtx);

    if (!exists_locked(t, id)) return WriteResult::not_found;
    if (log && !log()) return WriteResult::log_failed;

    t->index.erase(id); 

    std::vector<char> tombstone;
    tombstone.insert(tombstone.end(), reinterpret_cast<char*>(&id), reinterpret_cast<char*>(&id) + sizeof(int));
    insert_to_block(t, tombstone);
    return WriteResult::ok;
}
