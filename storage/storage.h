#pragma once
#include <string>
#include <unordered_map>
#include <map>
#include <memory>
#include <functional>
#include <set>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <fstream>
#include <filesystem>
#include <zlib.h>

#include "json.h"
#include "metrics.h"
#include "block_cache.h"

#include <atomic>

namespace fs = std::filesystem;

// Поддерживаемые типы данных
enum class DataType { STRING, INT, DOUBLE, BOOL, DATE };

// Заголовок блока, который будет записываться в файл
struct CompressedBlockHeader {
    uint32_t original_size;   // Размер данных до сжатия
    uint32_t compressed_size; // Размер данных после сжатия
};

struct Column {
    std::string name;
    DataType type;
};

#pragma pack(push, 1)
struct BinaryHeader {
    uint32_t magic = 0x4A423031;
    uint32_t total_size;
    uint32_t keys_count;
};

struct KeyRecord {
    uint32_t key_hash;
    uint32_t data_offset;
    uint32_t data_size;
};
#pragma pack(pop)

struct FileLocation {
    std::string filename;
    std::streampos offset;
};

struct Table {
    std::string name;
    std::string path;
    int current_seg_id = 0;
    std::vector<Column> schema; // Тот самый бинарный паспорт в памяти
    std::unordered_map<int, FileLocation> index;

    // Разделяемый: чтения идут параллельно, запись — эксклюзивно.
    // Защищает всё, что ниже, а также schema и index.
    // Гарантий справедливости стандарт не даёт, поэтому поток читателей
    // теоретически может надолго отодвинуть писателя.
    std::shared_mutex mtx;
    // Блочное сжатие: записи копятся здесь, пока не наберётся размер блока
    // (Storage::block_size_), затем сжимаются и уходят на диск.
    std::vector<char> write_buffer;

    // Сегменты, в которые писали после последнего fsync. Блок, сброшенный
    // через ofstream, лежит в кеше ядра и потерю питания не переживёт.
    std::set<std::string> unsynced_segments;
};

// Итог операции записи. Коды, а не исключения: каждый из них — ожидаемый
// ответ клиенту, а не авария.
enum class WriteResult {
    ok,
    table_not_found,
    id_exists,       // INSERT для занятого id
    not_found,       // DELETE для несуществующего id
    invalid_json,    // тело записи — не корректный JSON-объект
    invalid_data,    // запись не прошла проверку типов по схеме
    log_failed,      // журнал не смог сохранить операцию — она не применена
    read_error       // текущую версию записи не удалось прочитать с диска
};

enum class WriteMode {
    insert,   // занятый id — ошибка
    update,   // как UPDATE в SQL: меняет переданные поля существующей записи
    upsert    // запись целиком: новая или новая версия существующей
};

// Статистика хранилища для команды STATS.
struct StorageStats {
    lite_db::Counter blocks_written;          // блоков сброшено на диск
    lite_db::Counter block_bytes_raw;         // их размер до сжатия (колоночная раскладка)
    lite_db::Counter block_bytes_compressed;  // их размер на диске
    lite_db::Counter blocks_read;             // блоков прочитано и распаковано
};

// Вызывается под замком таблицы после проверки данных, но до их применения.
// Возвращает false, если операцию сохранить не удалось: тогда она отменяется.
using WriteLog = std::function<bool()>;

class Storage {
private:
    // Порядок полей подобран так, чтобы не было дыр выравнивания: шарды кеша
    // выровнены по 64 байта (см. BlockCache), и поле с таким выравниванием
    // в середине класса оставляло бы перед собой пустые байты.

    // Распакованные блоки, прочитанные с диска. По умолчанию 32 МБ,
    // настраивается block_cache_mb в setup.yaml.
    BlockCache cache_{32u * 1024u * 1024u};

    // Потолок на размер блока. Размеры читаются из заголовка в файле, и
    // повреждённое значение не должно приводить к выделению гигабайтов.
    static constexpr uint32_t MAX_BLOCK_SIZE = 64u * 1024u * 1024u;
    const size_t MAX_SEG_SIZE = 10 * 1024 * 1024;

    // Сколько байт записей копить в буфере таблицы перед сжатием в блок.
    // Меняется только при чтении конфигурации, до начала работы сервера.
    std::atomic<size_t> block_size_{4096};

    std::string root_path;

    StorageStats stats_;

    // Таблицей владеет карта. Удаления таблиц нет, поэтому указатель,
    // полученный из карты, остаётся валидным до разрушения Storage — и его
    // можно использовать после того, как замок каталога отпущен.
    // unique_ptr вместо голого Table*: деструктор писать не нужно, а случайное
    // копирование Storage (и двойной delete) не скомпилируется.
    std::unordered_map<std::string, std::unique_ptr<Table>> tables;
    std::shared_mutex tables_mtx;

    // Поиск таблицы без изменения карты. Вызывается с захваченным tables_mtx.
    Table* find_table(const std::string& name);

    // Берёт замок каталога ровно на время поиска и сразу его отпускает.
    // Таблица дальше защищается своим собственным замком.
    Table* lookup_table(const std::string& name);

    // Поля записи по имени. std::map — чтобы порядок полей в записи
    // не зависел от порядка в присланном JSON.
    using FieldMap = std::map<std::string, lite_db::json::Value>;

    uint32_t hash_string(const std::string& s);
    // Колонка "id" в схеме — тот же ключ записи: заполняет её, если не передана.
    void fill_id_column(Table* t, int id, FieldMap& fields);
    // id + упакованные поля — запись в формате буфера таблицы.
    std::vector<char> make_record(int id, const FieldMap& fields);

    // Проверяет поля по схеме таблицы (типы, набор колонок) и приводит
    // BOOL к true/false. false — данные не соответствуют схеме.
    bool validate_fields(Table* t, FieldMap& fields);
    std::vector<char> pack_fields(const FieldMap& fields);

    // Новые методы для работы с метаданными
    void save_schema(Table* t);
    void load_schema(Table* t);

    void load_table_index(Table* t);

    // Единственная точка распаковки блока: zlib -> колонки -> строки.
    bool read_block(std::ifstream& in, Table* t, std::vector<char>& row_out);

    // Чтение одного поля записи по имени, без разбора записи целиком.
    bool extract_field(const char* data_ptr, size_t payload_size,
                       const std::string& key, std::string& out);

    // Сборка JSON записи из значений полей по схеме таблицы.
    std::string rebuild_json(const char* data_ptr, size_t payload_size, Table* t);

    // Вырезает поле из записи и возвращает его значение через value.
    std::vector<char> strip_field(const char* payload, size_t payload_size,
                                  uint32_t field_hash, std::string& value, bool& found);

    // Возвращает вырезанное поле обратно в запись.
    std::vector<char> insert_field(const char* payload, size_t payload_size,
                                   uint32_t field_hash, const std::string& value);

    // --- НОВЫЙ КОНВЕЙЕР ---
    std::vector<char> pack_bools(const std::vector<char>& bool_bytes);
    std::vector<char> unpack_bools(const std::vector<char>& packed, size_t original_count);

    std::vector<char> pack_columns(Table* t);
    std::vector<char> unpack_columns(const std::vector<char>& columnar_buffer, Table* t);

    enum class Lookup { found, missing, read_error };

    // Последняя версия записи (буфер, затем диск). Вызывается с захваченным
    // t->mtx — хотя бы разделяемо.
    Lookup find_latest_locked(Table* t, int id, std::vector<char>& payload);

    // Вызываются с захваченным эксклюзивно t->mtx.
    bool exists_locked(Table* t, int id);
    void insert_to_block(Table* t, const std::vector<char>& raw_record);
    // false — блок записать не удалось; буфер и индекс при этом не меняются.
    bool flush_block_to_disk(Table* t);
    bool sync_segments(Table* t);

public:
    // root — каталог с данными (по умолчанию data/ в текущей папке).
    explicit Storage(std::string root = "data/");

    bool create_table(const std::string& name);
    bool set_schema(const std::string& table_name, const std::vector<Column>& columns);

    // Проверка «id занят?», журнал и применение идут под одним замком таблицы,
    // поэтому между проверкой и вставкой никто не успеет вклиниться.
    WriteResult insert(const std::string& table_name, int id, const std::string& json_str,
                       WriteMode mode = WriteMode::upsert, const WriteLog& log = {});
    WriteResult remove(const std::string& table_name, int id, const WriteLog& log = {});

    // Пакетная вставка (MPUT): все строки под одним замком таблицы, одной
    // записью журнала и одним fsync. Атомарна: если хоть одна строка не
    // проходит проверку или её id занят, не вставляется ничего.
    // skip_existing — для восстановления из журнала: строки, которые уже
    // попали на диск до аварии, пропускаются, а не отвергают весь пакет.
    using BatchRows = std::vector<std::pair<int, lite_db::json::Fields>>;
    WriteResult insert_batch(const std::string& table_name, const BatchRows& rows,
                             bool skip_existing, const WriteLog& log = {});

    std::string select(const std::string& table_name, int id, const std::string& target_key = "");
    // Ответ в одну строку: протокол разделяет ответы переводом строки.
    std::string select_all(const std::string& table_name);
    bool exists(const std::string& table_name, int id);

    const StorageStats& stats() const noexcept { return stats_; }
    uint64_t cache_hits() const noexcept { return cache_.hits(); }
    uint64_t cache_misses() const noexcept { return cache_.misses(); }

    // Настройки из setup.yaml. Вызывать до того, как сервер начал работу.
    void set_block_cache_bytes(size_t bytes) { cache_.set_capacity(bytes); }
    void set_block_size(size_t bytes) { block_size_.store(bytes); }
    size_t block_size() const noexcept { return block_size_.load(); }

    // Сбрасывает буферы всех таблиц и делает fsync сегментов.
    // Только после true журнал предзаписи можно очищать.
    [[nodiscard]] bool flush_all();
};
