#pragma once

#include <string>

// Проталкивание файлов на устройство.
//
// Запись через ofstream/fwrite заканчивается в кеше ядра: она переживёт
// падение процесса, но не потерю питания. Чтобы данные действительно
// оказались на диске, нужен fsync (POSIX) или FlushFileBuffers (Windows).

namespace lite_db {

// fsync файла по пути. Срабатывает и для данных, записанных через другой
// дескриптор: ядро сбрасывает все грязные страницы этого файла.
[[nodiscard]] bool sync_file(const std::string& path) noexcept;

// fsync каталога: делает долговечной саму запись о новом файле.
// Без него только что созданный сегмент может «исчезнуть» после сбоя
// питания, даже если его содержимое было сброшено. На Windows не требуется.
[[nodiscard]] bool sync_directory(const std::string& path) noexcept;

} // namespace lite_db
