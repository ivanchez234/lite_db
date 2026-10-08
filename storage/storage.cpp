#include "storage.h"
#include "byte_reader.h"
#include "file_sync.h"
#include "json.h"

#include <charconv>
#include <cmath>
#include <cstdlib>

#include <shared_mutex>
#include <iostream>
#include <map>
#include <sstream>
#include <algorithm>
#include <fstream>
#include <cstring> // Для memcpy
#include <zlib.h>



Storage::Storage(std::string root) : root_path(std::move(root)) {
    if (root_path.empty()) root_path = "data/";
    if (root_path.back() != '/') root_path += '/';
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

    stats_.blocks_read.add();
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

        // Имена и строки — через экранирование: значение может содержать
        // кавычки, обратные косые черты и т. п.
        out += lite_db::json::quote(col.name);
        out += ':';

        if (col.type == DataType::STRING || col.type == DataType::DATE) {
            out += lite_db::json::quote(val);
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

    if (t->write_buffer.size() >= block_size_.load(std::memory_order_relaxed)) {
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
    stats_.blocks_written.add();
    stats_.block_bytes_raw.add(original_size);
    stats_.block_bytes_compressed.add(sizeof(header) + compressed_size);
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

namespace {

bool is_leap_year(int year) noexcept {
    return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

// YYYY-MM-DD по настоящему календарю: 2023-02-29 — ошибка, 2024-02-29 — нет.
bool is_valid_date(const std::string& date) {
    if (date.size() != 10 || date[4] != '-' || date[7] != '-') return false;
    for (size_t i = 0; i < date.size(); ++i) {
        if (i == 4 || i == 7) continue;
        if (date[i] < '0' || date[i] > '9') return false;
    }

    const int year  = std::stoi(date.substr(0, 4));
    const int month = std::stoi(date.substr(5, 2));
    const int day   = std::stoi(date.substr(8, 2));
    if (month < 1 || month > 12 || day < 1) return false;

    static constexpr int kDaysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const int max_day = (month == 2 && is_leap_year(year)) ? 29 : kDaysInMonth[month - 1];
    return day <= max_day;
}

// Целое int32 целиком. Раньше проверка шла через std::stoi, который читает
// начало строки и молча отбрасывает хвост: "30abc" проходило как 30.
bool is_int32(const std::string& text) {
    if (!lite_db::json::is_number(text)) return false;
    if (text.find_first_of(".eE") != std::string::npos) return false;
    int value = 0;
    const char* const end = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(text.data(), end, value);
    return ec == std::errc() && ptr == end;
}

// Конечное число: 1e999 по грамматике JSON — число, но это бесконечность.
bool is_finite_double(const std::string& text) {
    if (!lite_db::json::is_number(text)) return false;
    char* end = nullptr;
    const double value = std::strtod(text.c_str(), &end);
    return end == text.c_str() + text.size() && std::isfinite(value);
}

// В протоколе перевод строки — конец ответа. Значение с переводом строки
// разорвало бы ответ на два, поэтому такие значения не принимаются.
bool has_line_break(const std::string& text) {
    return text.find_first_of("\r\n") != std::string::npos;
}

} // namespace

bool Storage::validate_fields(Table* t, FieldMap& fields) {
    for (const auto& field : fields) {
        if (field.second.is_string && has_line_break(field.second.text)) return false;
    }

    // Без схемы проверять нечего: такая таблица хранит любые поля.
    if (t->schema.empty()) return true;

    // Ровно колонки схемы. Лишнее поле иначе молча сохранилось бы
    // и никогда не вернулось бы в SELECT.
    if (fields.size() != t->schema.size()) return false;

    for (const auto& col : t->schema) {
        const auto it = fields.find(col.name);
        if (it == fields.end()) return false;
        lite_db::json::Value& value = it->second;

        switch (col.type) {
            case DataType::STRING:
                if (!value.is_string) return false;
                break;
            case DataType::DATE:
                if (!value.is_string || !is_valid_date(value.text)) return false;
                break;
            case DataType::INT:
                if (value.is_string || !is_int32(value.text)) return false;
                break;
            case DataType::DOUBLE:
                if (value.is_string || !is_finite_double(value.text)) return false;
                break;
            case DataType::BOOL:
                // SQL присылает 1/0, JSON — true/false. Храним единообразно.
                if (value.is_string) return false;
                if (value.text == "true" || value.text == "1")       value.text = "true";
                else if (value.text == "false" || value.text == "0") value.text = "false";
                else return false;
                break;
        }
    }
    return true;
}

std::vector<char> Storage::pack_fields(const FieldMap& fields) {
    BinaryHeader head;
    head.keys_count = static_cast<uint32_t>(fields.size());

    std::vector<KeyRecord> records;
    std::vector<char> body;
    uint32_t offset = 0;

    for (const auto& [name, value] : fields) {
        KeyRecord r;
        r.key_hash = hash_string(name);
        r.data_offset = offset;
        r.data_size = static_cast<uint32_t>(value.text.size());
        records.push_back(r);
        body.insert(body.end(), value.text.begin(), value.text.end());
        offset += static_cast<uint32_t>(value.text.size());
    }

    head.total_size = static_cast<uint32_t>(sizeof(BinaryHeader) + records.size() * sizeof(KeyRecord) + body.size());

    std::vector<char> buf;
    buf.insert(buf.end(), (char*)&head, (char*)&head + sizeof(BinaryHeader));
    for (auto& r : records) buf.insert(buf.end(), (char*)&r, (char*)&r + sizeof(KeyRecord));
    buf.insert(buf.end(), body.begin(), body.end());
    return buf;
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
    // Распаковка блока зависит от схемы (какая колонка — BOOL),
    // поэтому закешированные блоки после смены схемы недействительны.
    cache_.clear();
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
    // Разбор не зависит от таблицы, поэтому идёт до всех замков.
    lite_db::json::Fields parsed;
    std::string parse_error;
    if (!lite_db::json::parse_object(json_str, parsed, parse_error)) return WriteResult::invalid_json;
    FieldMap fields(parsed.begin(), parsed.end());

    Table* t = lookup_table(table_name);
    if (!t) return WriteResult::table_not_found;

    std::unique_lock<std::shared_mutex> t_lock(t->mtx);

    // Проверка и вставка под одним замком. Если проверить отдельно, два
    // INSERT с одним id могут оба увидеть «свободно» и оба записаться.
    if (mode == WriteMode::insert && exists_locked(t, id)) return WriteResult::id_exists;

    if (mode == WriteMode::update) {
        // Как UPDATE в SQL: меняются только переданные поля, остальные
        // берутся из текущей версии записи. У таблицы без схемы перечислить
        // старые поля нельзя (в записи лежат только хеши имён), поэтому
        // там UPDATE заменяет запись переданными полями.
        std::vector<char> current;
        const Lookup found = find_latest_locked(t, id, current);
        if (found == Lookup::missing)    return WriteResult::not_found;
        if (found == Lookup::read_error) return WriteResult::read_error;

        for (const auto& col : t->schema) {
            if (fields.count(col.name) != 0) continue;
            lite_db::json::Value old;
            if (!extract_field(current.data(), current.size(), col.name, old.text)) continue;
            old.is_string = (col.type == DataType::STRING || col.type == DataType::DATE);
            fields.emplace(col.name, std::move(old));
        }
    }

    fill_id_column(t, id, fields);

    // Сначала проверяем данные, потом пишем в журнал: мусор, который всё
    // равно будет отвергнут, не должен попадать в журнал.
    if (!validate_fields(t, fields)) return WriteResult::invalid_data;
    const std::vector<char> record = make_record(id, fields);

    // Журнал под замком таблицы: порядок записей в журнале совпадает
    // с порядком применения, и восстановление воспроизведёт ту же историю.
    if (log && !log()) return WriteResult::log_failed;

    insert_to_block(t, record);
    return WriteResult::ok;
}

void Storage::fill_id_column(Table* t, int id, FieldMap& fields) {
    // SQL-транслятор не кладёт id внутрь записи, поэтому заполняем его сами:
    // иначе таблица со схемой вида "id:INT name:STRING" отвергала бы каждую вставку.
    for (const auto& col : t->schema) {
        if (col.name == "id" && fields.count("id") == 0) {
            fields.emplace("id", lite_db::json::Value{std::to_string(id), false});
        }
    }
}

std::vector<char> Storage::make_record(int id, const FieldMap& fields) {
    const std::vector<char> bin = pack_fields(fields);
    std::vector<char> record;
    record.reserve(sizeof(int) + bin.size());
    record.insert(record.end(), reinterpret_cast<const char*>(&id), reinterpret_cast<const char*>(&id) + sizeof(int));
    record.insert(record.end(), bin.begin(), bin.end());
    return record;
}

WriteResult Storage::insert_batch(const std::string& table_name, const BatchRows& rows,
                                  bool skip_existing, const WriteLog& log) {
    // Повтор id внутри одного пакета — ошибка клиента. Проверяем заранее:
    // строки пакета попадают в буфер только в конце, и exists_locked их не видит.
    {
        std::unordered_map<int, bool> seen;
        for (const auto& row : rows) {
            if (!seen.emplace(row.first, true).second) return WriteResult::id_exists;
        }
    }

    Table* t = lookup_table(table_name);
    if (!t) return WriteResult::table_not_found;

    std::unique_lock<std::shared_mutex> t_lock(t->mtx);

    // Сначала проверяем ВСЕ строки и только потом что-то меняем: пакет
    // применяется целиком или не применяется вовсе.
    std::vector<std::vector<char>> records;
    records.reserve(rows.size());
    for (const auto& [id, parsed] : rows) {
        if (exists_locked(t, id)) {
            if (skip_existing) continue;
            return WriteResult::id_exists;
        }
        FieldMap fields(parsed.begin(), parsed.end());
        fill_id_column(t, id, fields);
        if (!validate_fields(t, fields)) return WriteResult::invalid_data;
        records.push_back(make_record(id, fields));
    }

    if (records.empty()) return WriteResult::ok;   // при восстановлении всё уже было на диске

    // Весь пакет — одна запись журнала и, значит, один fsync.
    if (log && !log()) return WriteResult::log_failed;

    for (const auto& record : records) insert_to_block(t, record);
    return WriteResult::ok;
}

Storage::Lookup Storage::find_latest_locked(Table* t, int id, std::vector<char>& payload) {
    // Последняя версия записи — последняя по порядку: сначала в буфере
    // (он новее диска), потом в блоке на диске. Надгробие означает «удалена».
    auto scan = [id, &payload](const char* data, size_t size, bool& seen) {
        bool alive = false;
        size_t offset = 0;
        RecordView rec;
        while (next_record(data, size, offset, rec)) {
            if (rec.id != id) continue;
            seen  = true;
            alive = rec.payload_size != 0;
            if (alive) payload.assign(rec.payload, rec.payload + rec.payload_size);
        }
        return alive;
    };

    bool seen = false;
    const bool alive_in_buffer = scan(t->write_buffer.data(), t->write_buffer.size(), seen);
    if (seen) return alive_in_buffer ? Lookup::found : Lookup::missing;

    // Именно find, а не operator[]: тот не const и под разделяемым замком
    // стал бы изменением карты из нескольких потоков сразу.
    const auto index_it = t->index.find(id);
    if (index_it == t->index.end()) return Lookup::missing;

    const FileLocation& loc = index_it->second;
    const auto offset = static_cast<int64_t>(static_cast<std::streamoff>(loc.offset));

    // Сначала кеш: блок на диске неизменен, поэтому копия не устаревает.
    BlockCache::Rows rows = cache_.get(loc.filename, offset);
    if (!rows) {
        std::ifstream in(loc.filename, std::ios::binary);
        if (!in) return Lookup::read_error;
        in.seekg(loc.offset);

        // read_block проверяет размеры из заголовка до выделения памяти.
        auto fresh = std::make_shared<std::vector<char>>();
        if (!read_block(in, t, *fresh)) return Lookup::read_error;
        rows = std::move(fresh);
        cache_.put(loc.filename, offset, rows);
    }

    return scan(rows->data(), rows->size(), seen) ? Lookup::found : Lookup::missing;
}

std::string Storage::select(const std::string& table_name, int id, const std::string& target_key) {
    Table* t = lookup_table(table_name);
    if (!t) return "ERR_TABLE_NOT_FOUND";

    // Чтение: несколько запросов к одной таблице идут одновременно.
    std::shared_lock<std::shared_mutex> t_lock(t->mtx);

    std::vector<char> payload;
    switch (find_latest_locked(t, id, payload)) {
        case Lookup::missing:    return "ERR_NOT_FOUND";
        case Lookup::read_error: return "ERR_READ_FAILED";
        case Lookup::found:      break;
    }

    if (!target_key.empty()) {
        std::string value;
        if (!extract_field(payload.data(), payload.size(), target_key, value)) return "ERR_KEY_NOT_FOUND";
        return value;
    }

    const std::string json = rebuild_json(payload.data(), payload.size(), t);
    return json.empty() ? "ERR_NO_SCHEMA" : json;
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
