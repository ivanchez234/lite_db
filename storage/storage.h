#pragma once
#include <string>
#include <unordered_map>
#include <mutex>
#include <filesystem>

namespace fs = std::filesystem;

// Структура для хранения точного адреса данных
struct FileLocation {
    std::string filename;
    std::streampos offset; // Смещение в байтах от начала файла
};

class Storage {
private:
    std::string base_name = "seg_";
    int current_seg_id = 0;
    const size_t MAX_SEG_SIZE = 5120; // Твои 5 КБ

    // Теперь индекс хранит и файл, и позицию в нем
    std::unordered_map<int, FileLocation> index;
    std::mutex mtx;

    std::string get_seg_name(int id) { return base_name + std::to_string(id) + ".db"; }
    void load_index(); 
    size_t get_file_size(const std::string& filename);

public:
    Storage();
    void insert(int key, const std::string& value);
    std::string select(int key);
    void remove(int key);
    bool exists(int key) { return index.count(key); }
};