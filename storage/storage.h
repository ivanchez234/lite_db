#pragma once
#include <string>
#include <unordered_map>
#include <mutex>

class Storage {
private:
    std::string filename;
    std::unordered_map<int, std::string> data;
    std::mutex mtx; // Для потокобезопасности

    void load();
    void save();
public:
    Storage(const std::string& file);
    void insert(int key, const std::string& value);
    std::string select(int key);
    void remove(int key);
    bool exists(int key);
};