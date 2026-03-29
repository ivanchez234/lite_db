#pragma once
#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <filesystem>

namespace fs = std::filesystem;

class Storage {
private:
    std::string base_name = "seg_";
    int current_seg_id = 0;
    size_t MAX_SEG_SIZE = 5120; // 5 KB

    // Индекс: ID -> Имя файла
    std::unordered_map<int, std::string> index;
    std::mutex mtx;

    std::string get_seg_name(int id) { return base_name + std::to_string(id) + ".db"; }
    void load_index(); // Просканировать все файлы и заполнить индекс
    size_t get_file_size(const std::string& filename);

public:
    Storage();
    void insert(int key, const std::string& value);
    std::string select(int key);
    void remove(int key);
    bool exists(int key) { return index.count(key); }
};