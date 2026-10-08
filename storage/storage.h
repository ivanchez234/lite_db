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
    // Блочное сжатие
    std::vector<char> write_buffer;
    const size_t BLOCK_SIZE = 4096; // 4 КБ — стандартный размер блока

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
    invalid_data,    // запись не прошла проверку типов по схеме
    log_failed       // журнал не смог сохранить операцию — она не применена
};

enum class WriteMode {
    insert,   // занятый id — ошибка
    upsert    // занятый id — новая версия записи (так работает UPDATE)
};

// Вызывается под замком таблицы после проверки данных, но до их применения.
// Возвращает false, если операцию сохранить не удалось: тогда она отменяется.
using WriteLog = std::function<bool()>;

class Storage {
private:
    std::string root_path = "data/";
    // Потолок на размер блока. Размеры читаются из заголовка в файле, и
    // повреждённое значение не должно приводить к выделению гигабайтов.
    static constexpr uint32_t MAX_BLOCK_SIZE = 64u * 1024u * 1024u;
    const size_t MAX_SEG_SIZE = 10 * 1024 * 1024;

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

    std::map<std::string, std::string> parse_json_manual(std::string s);
    uint32_t hash_string(const std::string& s);
    // false — данные не соответствуют схеме таблицы.
    bool pack_json(const std::string& json_str, Table* t, std::vector<char>& out);

    // Новые методы для работы с метаданными
    void save_schema(Table* t);
    void load_schema(Table* t);
    bool validate_types(Table* t, const std::map<std::string, std::string>& data);

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

    // Все три вызываются с захваченным эксклюзивно t->mtx.
    bool exists_locked(Table* t, int id);
    void insert_to_block(Table* t, const std::vector<char>& raw_record);
    // false — блок записать не удалось; буфер и индекс при этом не меняются.
    bool flush_block_to_disk(Table* t);
    bool sync_segments(Table* t);

public:
    Storage();

    bool create_table(const std::string& name);
    bool set_schema(const std::string& table_name, const std::vector<Column>& columns);

    // Проверка «id занят?», журнал и применение идут под одним замком таблицы,
    // поэтому между проверкой и вставкой никто не успеет вклиниться.
    WriteResult insert(const std::string& table_name, int id, const std::string& json_str,
                       WriteMode mode = WriteMode::upsert, const WriteLog& log = {});
    WriteResult remove(const std::string& table_name, int id, const WriteLog& log = {});

    std::string select(const std::string& table_name, int id, const std::string& target_key = "");
    // Ответ в одну строку: протокол разделяет ответы переводом строки.
    std::string select_all(const std::string& table_name);
    bool exists(const std::string& table_name, int id);

    // Сбрасывает буферы всех таблиц и делает fsync сегментов.
    // Только после true журнал предзаписи можно очищать.
    [[nodiscard]] bool flush_all();
};
