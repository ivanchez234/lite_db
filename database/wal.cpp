#include "wal.h"

#include <zlib.h>

#ifdef _WIN32
    #include <io.h>
    #define LITE_DB_FILENO _fileno
    #define LITE_DB_FSYNC  _commit
#else
    #include <unistd.h>
    #define LITE_DB_FILENO fileno
    #define LITE_DB_FSYNC  fsync
#endif

namespace {

// Потолок на длину записи: длина читается из самого файла.
constexpr uint32_t kMaxRecordSize = 16u * 1024u * 1024u;

uint32_t checksum(const std::string& data) {
    return static_cast<uint32_t>(
        crc32(0L, reinterpret_cast<const Bytef*>(data.data()),
              static_cast<uInt>(data.size())));
}

} // namespace

Wal::Wal(std::string path, Sync mode)
    : path_(std::move(path)), mode_(mode) {
    file_ = std::fopen(path_.c_str(), "a+b");
}

Wal::~Wal() {
    if (file_) {
        push_to_disk();
        std::fclose(file_);
    }
}

bool Wal::append(const std::string& payload) {
    if (!file_) return false;
    if (payload.empty() || payload.size() > kMaxRecordSize) return false;

    const uint32_t len = static_cast<uint32_t>(payload.size());
    const uint32_t sum = checksum(payload);

    if (std::fseek(file_, 0, SEEK_END) != 0) return false;
    if (std::fwrite(&sum, sizeof(sum), 1, file_) != 1) return false;
    if (std::fwrite(&len, sizeof(len), 1, file_) != 1) return false;
    if (std::fwrite(payload.data(), 1, len, file_) != len) return false;

    return push_to_disk();
}

bool Wal::push_to_disk() {
    if (!file_) return false;

    // Буфер процесса: до диска запись ещё не дошла.
    if (mode_ == Sync::none) return true;

    // Отдали ядру: переживёт падение процесса, но не потерю питания.
    if (std::fflush(file_) != 0) return false;
    if (mode_ == Sync::flush) return true;

    // Попросили устройство записать по-настоящему.
    return LITE_DB_FSYNC(LITE_DB_FILENO(file_)) == 0;
}

std::vector<std::string> Wal::replay() const {
    std::vector<std::string> records;

    std::FILE* in = std::fopen(path_.c_str(), "rb");
    if (!in) return records;

    for (;;) {
        uint32_t sum = 0;
        uint32_t len = 0;

        if (std::fread(&sum, sizeof(sum), 1, in) != 1) break;   // конец файла
        if (std::fread(&len, sizeof(len), 1, in) != 1) break;   // заголовок оборван
        if (len == 0 || len > kMaxRecordSize) break;            // длина бессмысленна

        std::string payload(len, '\0');
        if (std::fread(&payload[0], 1, len, in) != len) break;  // запись оборвана

        if (checksum(payload) != sum) break;                    // запись повреждена

        records.push_back(std::move(payload));
    }

    std::fclose(in);
    return records;
}

void Wal::reset() {
    if (file_) {
        std::fclose(file_);
        file_ = nullptr;
    }

    // Открытие на запись обрезает файл. Отдельный дескриптор не держим:
    // два открытых дескриптора на один файл ведут себя по-разному в разных ОС.
    if (std::FILE* truncating = std::fopen(path_.c_str(), "wb")) {
        std::fclose(truncating);
    }

    file_ = std::fopen(path_.c_str(), "a+b");
    push_to_disk();
}

const char* Wal::sync_mode_name(Sync mode) noexcept {
    switch (mode) {
        case Sync::none:  return "none";
        case Sync::flush: return "flush";
        case Sync::full:  return "full";
    }
    return "full";
}

bool Wal::parse_sync_mode(const std::string& text, Sync& out) noexcept {
    if (text == "none")  { out = Sync::none;  return true; }
    if (text == "flush") { out = Sync::flush; return true; }
    if (text == "full")  { out = Sync::full;  return true; }
    return false;
}
