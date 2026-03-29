#include "storage.h"
#include <fstream>

Storage::Storage(const std::string& file) : filename(file) { load(); }

void Storage::load() {
    std::ifstream in(filename);
    int key; std::string value;
    while (in >> key) {
        std::getline(in >> std::ws, value);
        data[key] = value;
    }
}

void Storage::save() {
    std::ofstream out(filename);
    for (auto const& [key, value] : data) {
        out << key << " " << value << "\n";
    }
}

void Storage::insert(int key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mtx);
    data[key] = value;
    save();
}

std::string Storage::select(int key) {
    std::lock_guard<std::mutex> lock(mtx);
    return (data.count(key)) ? data[key] : "NULL";
}

void Storage::remove(int key) {
    std::lock_guard<std::mutex> lock(mtx);
    data.erase(key);
    save();
}

bool Storage::exists(int key) {
    std::lock_guard<std::mutex> lock(mtx);
    return data.find(key) != data.end();
}