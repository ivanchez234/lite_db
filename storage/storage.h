#pragma once
#include <string>
#include <unordered_map>
#include <map>
#include <vector>
#include <mutex>
#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;

// Поддерживаемые типы данных
enum class DataType { STRING, INT, DOUBLE, BOOL };

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
    std::mutex mtx;
};

class Storage {
private:
    std::string root_path = "data/";
    const size_t MAX_SEG_SIZE = 1024 * 1024;
    std::unordered_map<std::string, Table*> tables;
    std::mutex tables_mtx;

    std::map<std::string, std::string> parse_json_manual(std::string s);
    uint32_t hash_string(const std::string& s);
    std::vector<char> pack_json(const std::string& json_str, Table* t);
    
    // Новые методы для работы с метаданными
    void save_schema(Table* t);
    void load_schema(Table* t);
    bool validate_types(Table* t, const std::map<std::string, std::string>& data);
    
    // ДОБАВЬ ЭТУ СТРОКУ:
    void load_table_index(Table* t); 

public:
    Storage();
    ~Storage();

    bool create_table(const std::string& name);
    bool set_schema(const std::string& table_name, const std::vector<Column>& columns);
    void insert(const std::string& table_name, int id, const std::string& json_str);
    std::string select(const std::string& table_name, int id, const std::string& target_key = "");
    std::string select_all(const std::string& table_name);
    void remove(const std::string& table_name, int id);
    bool exists(const std::string& table_name, int id);
};