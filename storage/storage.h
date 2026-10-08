#pragma once
#include <string>
#include <unordered_map>
#include <map>
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
    // Гарантий справедливости стандарт не даёт, поэтому поток читателей
    // теоретически может надолго отодвинуть писателя.
    std::shared_mutex mtx;
    // Блочное сжатие
    std::vector<char> write_buffer; 
    const size_t BLOCK_SIZE = 4096; // 4 КБ — стандартный размер блока
};

class Storage {
private:
    std::string root_path = "data/";
    // Потолок на размер блока. Размеры читаются из заголовка в файле, и
    // повреждённое значение не должно приводить к выделению гигабайтов.
    static constexpr uint32_t MAX_BLOCK_SIZE = 64u * 1024u * 1024u;
    const size_t MAX_SEG_SIZE = 10 * 1024 * 1024;
    std::unordered_map<std::string, Table*> tables;
    std::shared_mutex tables_mtx;

    // Поиск таблицы без изменения карты. Вызывается с захваченным tables_mtx.
    Table* find_table(const std::string& name);

    std::map<std::string, std::string> parse_json_manual(std::string s);
    uint32_t hash_string(const std::string& s);
    std::vector<char> pack_json(const std::string& json_str, Table* t);
    
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

public:
    Storage();
    ~Storage();

    bool create_table(const std::string& name);
    bool set_schema(const std::string& table_name, const std::vector<Column>& columns);
    // false означает, что таблицы нет: молчаливый отказ выглядел бы как успех.
    bool insert(const std::string& table_name, int id, const std::string& json_str);
    std::string select(const std::string& table_name, int id, const std::string& target_key = "");
    std::string select_all(const std::string& table_name);
    void remove(const std::string& table_name, int id);
    bool exists(const std::string& table_name, int id);
    std::unordered_map<std::string, Table*>& get_all_tables() {
        return tables;
    }
// В класс Storage добавь эти методы:
    void flush_block_to_disk(Table* t);
    void insert_to_block(Table* t, const std::vector<char>& raw_record);
};