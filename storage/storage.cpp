#include "storage.h"
#include <fstream>
#include <iostream>

Storage::Storage() {
    load_index();
}

void Storage::load_index() {
    // Проходим по всем файлам seg_*.db и строим карту ключей
    for (int i = 0; ; ++i) {
        std::string name = get_seg_name(i);
        if (!fs::exists(name)) {
            current_seg_id = (i > 0) ? i - 1 : 0;
            break;
        }
        std::ifstream in(name);
        int key; std::string val;
        while (in >> key) {
            std::getline(in >> std::ws, val);
            index[key] = name; // Запоминаем, в каком файле этот ключ
        }
    }
}

size_t Storage::get_file_size(const std::string& filename) {
    if (!fs::exists(filename)) return 0;
    return fs::file_size(filename);
}

void Storage::insert(int key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mtx);

    std::string current_file = get_seg_name(current_seg_id);

    // Если текущий файл переполнен — переходим к следующему
    if (get_file_size(current_file) >= MAX_SEG_SIZE) {
        current_seg_id++;
        current_file = get_seg_name(current_seg_id);
    }

    // Записываем в конец файла (Append-only)
    std::ofstream out(current_file, std::ios::app);
    out << key << " " << value << "\n";
    
    // Обновляем индекс в памяти
    index[key] = current_file;
}

std::string Storage::select(int key) {
    std::lock_guard<std::mutex> lock(mtx);
    if (!index.count(key)) return "NULL";

    // Читаем ТОЛЬКО тот файл, который указан в индексе
    std::string target_file = index[key];
    std::ifstream in(target_file);
    int k; std::string v;
    std::string last_found = "NULL";

    while (in >> k) {
        std::getline(in >> std::ws, v);
        if (k == key) last_found = v; // Ищем последнее вхождение в этом файле
    }
    return last_found;
}

void Storage::remove(int key) {
    std::lock_guard<std::mutex> lock(mtx);
    index.erase(key);
    // В упрощенном логе мы просто удаляем из индекса. 
    // На диске данные останутся, но база их "забудет".
}