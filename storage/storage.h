#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <fstream>

#pragma pack(push, 1) // Отключаем лишние отступы в структурах
struct BinaryHeader {
    uint32_t magic = 0x4A423031; // "JB01"
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

class Storage {
private:
    std::string base_name = "seg_";
    int current_seg_id = 0;
    const size_t MAX_SEG_SIZE = 1024 * 1024; // 1 MB сегмент
    std::unordered_map<int, FileLocation> index;
    std::mutex mtx;

    uint32_t hash_string(const std::string& s);
    std::vector<char> pack_json(const std::string& json_str);
    void load_index();

public:
    Storage();
    void insert(int id, const std::string& json_str);
    std::string select(int id, const std::string& target_key = "");
    void remove(int id);
    bool exists(int id);
};