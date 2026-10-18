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

#include "json.h"
#include "metrics.h"
#include "block_cache.h"

#include <atomic>

namespace fs = std::filesystem;

enum class DataType { STRING, INT, DOUBLE, BOOL, DATE };

// Заголовок блока, который будет записываться в файл
struct CompressedBlockHeader {
    uint32_t original_size;
    uint32_t compressed_size;
};
// Колонка схемы таблицы: имя и тип
struct Column {
    std::string name;
    DataType type;
};

#pragma pack(push, 1)
// Заголовок записи (payload): метка формата, полный размер, число полей
struct BinaryHeader {
    uint32_t magic = 0x4A423031;
    uint32_t total_size;
    uint32_t keys_count;
};
// Строка оглавления записи: хеш имени поля, где лежит его значение и какой длины
struct KeyRecord {
    uint32_t key_hash;
    uint32_t data_offset;
    uint32_t data_size;
};
#pragma pack(pop)
// Где на диске лежит блок с записью: файл сегмента и смещение блока в нём
struct FileLocation {
    std::string filename;
    std::streampos offset;
};
// Таблица в памяти: схема, индекс id → блок, буфер новых записей и её замок
struct Table {
    std::string name;
    std::string path;
    int current_seg_id = 0;
    std::vector<Column> schema; 
    std::unordered_map<int, FileLocation> index;

    std::shared_mutex mtx;
    std::vector<char> write_buffer;

    std::set<std::string> unsynced_segments;
};
// Результат операции записи: код, который превратится в ответ клиенту
enum class WriteResult {
    ok,
    table_not_found,
    id_exists,       
    not_found,     
    invalid_json,    
    invalid_data,   
    log_failed,      
    read_error,
    field_hash_collision     
};

// Итог установки схемы таблицы.
enum class SchemaResult {
    ok,
    table_not_found,
    duplicate_column,  
    hash_collision  
};
// Режим записи: только новая запись, изменение существующей или «вставить или заменить»
enum class WriteMode {
    insert, 
    update,  
    upsert    
};

// Статистика хранилища для команды STATS.
struct StorageStats {
    lite_db::Counter blocks_written;         
    lite_db::Counter block_bytes_raw;         
    lite_db::Counter block_bytes_compressed;
    lite_db::Counter blocks_read;           
};


using WriteLog = std::function<bool()>;
class Storage {
private:
    // Кеш распакованных блоков: повторное чтение того же блока не идёт на диск
    BlockCache cache_{32u * 1024u * 1024u};

    // Потолок для размеров, прочитанных из файла, и порог перехода на новый сегмент
    static constexpr uint32_t MAX_BLOCK_SIZE = 64u * 1024u * 1024u;
    static constexpr size_t MAX_SEG_SIZE = 10 * 1024 * 1024;

    // Сколько байт копить в write_buffer, прежде чем сжать их в блок
    std::atomic<size_t> block_size_{4096};

    std::string root_path;

    StorageStats stats_;

    // Каталог таблиц и его замок (берётся только на время поиска или создания таблицы)
    std::unordered_map<std::string, std::unique_ptr<Table>> tables;
    std::shared_mutex tables_mtx;

    Table* find_table(const std::string& name);
    Table* lookup_table(const std::string& name);

    // Запись: поля из JSON в байты записи set_schema
    using FieldMap = std::map<std::string, lite_db::json::Value>;

    static uint32_t hash_string(const std::string& s);
    void fill_id_column(Table* t, int id, FieldMap& fields);
    std::vector<char> make_record(int id, const FieldMap& fields);   

    bool validate_fields(Table* t, FieldMap& fields);
    bool has_field_hash_collision(const FieldMap& fields);                
    std::vector<char> pack_fields(const FieldMap& fields);      

    // Схема и индекс: сохраняются на диск и восстанавливаются при запуске
    void save_schema(Table* t);
    void load_schema(Table* t);

    void load_table_index(Table* t);   

    // Чтение: блок с диска → строки → нужное поле
    bool read_block(std::ifstream& in, Table* t, std::vector<char>& row_out);   

    bool extract_field(const char* data_ptr, size_t payload_size,
                       const std::string& key, std::string& out);   // одно поле по хешу имени

    std::string rebuild_json(const char* data_ptr, size_t payload_size, Table* t);   // вся запись как JSON

    // Колоночная упаковка блока
    std::vector<char> strip_field(const char* payload, size_t payload_size,
                                  uint32_t field_hash, std::string& value, bool& found);

    std::vector<char> insert_field(const char* payload, size_t payload_size,
                                   uint32_t field_hash, const std::string& value);

    std::vector<char> pack_bools(const std::vector<char>& bool_bytes);
    std::vector<char> unpack_bools(const std::vector<char>& packed, size_t original_count);

    std::vector<char> pack_columns(Table* t);   // строки → колонки перед сжатием
    std::vector<char> unpack_columns(const std::vector<char>& columnar_buffer, Table* t);   // обратно

    enum class Lookup { found, missing, read_error };

    Lookup find_latest_locked(Table* t, int id, std::vector<char>& payload);   

    bool exists_locked(Table* t, int id);
    void insert_to_block(Table* t, const std::vector<char>& raw_record);  
    bool flush_block_to_disk(Table* t);   
    bool sync_segments(Table* t);        

public:
    explicit Storage(std::string root = "data/");

    bool create_table(const std::string& name);
    SchemaResult set_schema(const std::string& table_name, const std::vector<Column>& columns);

    // Запись: проверка → журнал (log) → write_buffer, всё под одним замком таблицы
    WriteResult insert(const std::string& table_name, int id, const std::string& json_str,
                       WriteMode mode = WriteMode::upsert, const WriteLog& log = {});
    WriteResult remove(const std::string& table_name, int id, const WriteLog& log = {});   // дописывает надгробие

    // MPUT: атомарный пакет, одна запись в журнал; skip_existing нужен при восстановлении
    using BatchRows = std::vector<std::pair<int, lite_db::json::Fields>>;
    WriteResult insert_batch(const std::string& table_name, const BatchRows& rows,
                             bool skip_existing, const WriteLog& log = {});

    // Чтение: под разделяемым замком таблицы, читатели идут параллельно
    std::string select(const std::string& table_name, int id, const std::string& target_key = "");
    std::string select_all(const std::string& table_name);
    bool exists(const std::string& table_name, int id);

    // Статистика для STATS
    const StorageStats& stats() const noexcept { return stats_; }
    uint64_t cache_hits() const noexcept { return cache_.hits(); }
    uint64_t cache_misses() const noexcept { return cache_.misses(); }

    // Настройки из setup.yaml, до старта сервера
    void set_block_cache_bytes(size_t bytes) { cache_.set_capacity(bytes); }
    void set_block_size(size_t bytes) { block_size_.store(bytes); }
    size_t block_size() const noexcept { return block_size_.load(); }

    // FLUSH: все буферы на диск + fsync; только после true можно чистить WAL
    [[nodiscard]] bool flush_all();
};