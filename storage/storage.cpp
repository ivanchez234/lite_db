#include "storage.h"
#include <fstream>
#include <iostream>

Storage::Storage() {
    load_index();
}

void Storage::load_index() {
    for (int i = 0; ; ++i) {
        std::string name = get_seg_name(i);
        if (!fs::exists(name)) {
            current_seg_id = (i > 0) ? i - 1 : 0;
            break;
        }
        std::ifstream in(name, std::ios::binary);
        int key;
        std::string val;
        
        while (in.peek() != EOF) {
            std::streampos pos = in.tellg(); // Запоминаем, где началась строка
            if (in >> key) {
                std::getline(in >> std::ws, val);
                index[key] = { name, pos }; // Сохраняем путь и смещение
            }
        }
    }
}

size_t Storage::get_file_size(const std::string& filename) {
    if (!fs::exists(filename)) return 0;
    return (size_t)fs::file_size(filename);
}

void Storage::insert(int key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mtx);
    std::string current_file = get_seg_name(current_seg_id);

    if (get_file_size(current_file) >= MAX_SEG_SIZE) {
        current_seg_id++;
        current_file = get_seg_name(current_seg_id);
    }

    // Открываем для дозаписи
    std::ofstream out(current_file, std::ios::app | std::ios::binary);
    
    // ВАЖНО: Получаем позицию ПЕРЕД записью
    out.seekp(0, std::ios::end); 
    std::streampos pos = out.tellp(); 

    out << key << " " << value << "\n";
    
    // Обновляем хеш-индекс в памяти
    index[key] = { current_file, pos };
}

std::string Storage::select(int key) {
    std::lock_guard<std::mutex> lock(mtx);
    if (index.find(key) == index.end()) return "NULL";

    FileLocation loc = index[key];
    std::ifstream in(loc.filename, std::ios::binary);
    
    // МАГИЯ ХЕША: Прыгаем сразу по адресу
    in.seekg(loc.offset); 

    int k;
    std::string v;
    if (in >> k) {
        std::getline(in >> std::ws, v);
        return v; // Нашли мгновенно!
    }

    return "NULL";
}

void Storage::remove(int key) {
    std::lock_guard<std::mutex> lock(mtx);
    index.erase(key); // Просто забываем адрес. В файле данные "умрут" при очистке.
}