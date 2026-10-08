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
    : path_(std::move(path)), mode_(mode), file_(std::fopen(path_.c_str(), "a+b")) {}

Wal::~Wal() {
    std::unique_lock<std::mutex> lock(mutex_);
    synced_cv_.wait(lock, [this] { return !syncing_; });
    if (file_) push_to_disk();
    // Сам файл закроет FilePtr.
}

bool Wal::is_open() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return file_ != nullptr;
}

bool Wal::healthy() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return file_ != nullptr && !failed_;
}

Wal::Sync Wal::sync_mode() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return mode_;
}

void Wal::set_sync_mode(Sync mode) {
    std::lock_guard<std::mutex> lock(mutex_);
    mode_ = mode;
}

bool Wal::append(const std::string& payload) {
    if (payload.empty() || payload.size() > kMaxRecordSize) return false;

    std::unique_lock<std::mutex> lock(mutex_);
    if (!file_ || failed_) return false;

    if (!write_record(payload)) {
        failed_ = true;
        return false;
    }
    appends_.add();
    bytes_.add(sizeof(uint32_t) * 2 + payload.size());

    if (mode_ == Sync::none) return true;

    if (mode_ == Sync::flush) {
        if (std::fflush(file_.get()) == 0) return true;
        failed_ = true;
        return false;
    }

    // --- Групповой коммит (режим full) ---
    //
    // fsync стоит сотни микросекунд, и раньше каждая запись делала свой,
    // держа мьютекс журнала: N параллельных записей ждали N fsync по очереди.
    //
    // Теперь первый, кто застал журнал без идущего fsync, становится лидером:
    // проталкивает в ядро всё дописанное к этому моменту (своё и чужое)
    // и делает ОДИН fsync уже без мьютекса. Пока он ждёт диск, другие потоки
    // дописывают свои записи и встают в очередь. Когда лидер закончил, все,
    // чьи записи попали в его fsync, получают ответ сразу; остальные выбирают
    // нового лидера. Один fsync подтверждает сразу пачку записей.
    const uint64_t my_seq = ++written_seq_;

    for (;;) {
        if (failed_) return false;              // чужой fsync упал — нашей записи верить нельзя
        if (synced_seq_ >= my_seq) return true; // нашу запись уже подтвердил чей-то fsync

        if (syncing_) {
            synced_cv_.wait(lock);
            continue;
        }

        syncing_ = true;
        const uint64_t target = written_seq_;   // всё, что дописано к этому моменту
        bool ok = std::fflush(file_.get()) == 0;
        const int fd = LITE_DB_FILENO(file_.get());

        lock.unlock();
        if (ok) {
            fsyncs_.add();
            ok = LITE_DB_FSYNC(fd) == 0;
        }
        lock.lock();

        syncing_ = false;
        if (ok) synced_seq_ = target;
        else    failed_ = true;
        synced_cv_.notify_all();
    }
}

bool Wal::write_record(const std::string& payload) {
    const uint32_t len = static_cast<uint32_t>(payload.size());
    const uint32_t sum = checksum(payload);

    std::FILE* f = file_.get();
    if (std::fseek(f, 0, SEEK_END) != 0) return false;
    if (std::fwrite(&sum, sizeof(sum), 1, f) != 1) return false;
    if (std::fwrite(&len, sizeof(len), 1, f) != 1) return false;
    if (std::fwrite(payload.data(), 1, len, f) != len) return false;
    return true;
}

bool Wal::push_to_disk() {
    if (!file_) return false;

    // Буфер процесса: до диска запись ещё не дошла.
    if (mode_ == Sync::none) return true;

    // Отдали ядру: переживёт падение процесса, но не потерю питания.
    if (std::fflush(file_.get()) != 0) return false;
    if (mode_ == Sync::flush) return true;

    // Попросили устройство записать по-настоящему.
    fsyncs_.add();
    return LITE_DB_FSYNC(LITE_DB_FILENO(file_.get())) == 0;
}

std::vector<std::string> Wal::replay() const {
    std::lock_guard<std::mutex> lock(mutex_);

    // Свои записи могли застрять в буфере процесса (режим none) —
    // отдаём их ядру, иначе отдельный дескриптор ниже их не увидит.
    if (file_) (void)std::fflush(file_.get());

    std::vector<std::string> records;

    const FilePtr in(std::fopen(path_.c_str(), "rb"));
    if (!in) return records;

    for (;;) {
        uint32_t sum = 0;
        uint32_t len = 0;

        if (std::fread(&sum, sizeof(sum), 1, in.get()) != 1) break;   // конец файла
        if (std::fread(&len, sizeof(len), 1, in.get()) != 1) break;   // заголовок оборван
        if (len == 0 || len > kMaxRecordSize) break;                  // длина бессмысленна

        std::string payload(len, '\0');
        if (std::fread(&payload[0], 1, len, in.get()) != len) break;  // запись оборвана

        if (checksum(payload) != sum) break;                          // запись повреждена

        records.push_back(std::move(payload));
    }

    return records;
}

bool Wal::reset() {
    std::unique_lock<std::mutex> lock(mutex_);
    // На всякий случай дожидаемся идущего fsync: файл сейчас будет закрыт.
    // (FLUSH и так держит замок контрольной точки, и пишущих в этот момент нет.)
    synced_cv_.wait(lock, [this] { return !syncing_; });

    file_.reset();

    // Открытие на запись обрезает файл. Отдельный дескриптор не держим:
    // два открытых дескриптора на один файл ведут себя по-разному в разных ОС.
    if (const FilePtr truncating{std::fopen(path_.c_str(), "wb")}; !truncating) {
        failed_ = true;
        return false;
    }

    file_.reset(std::fopen(path_.c_str(), "a+b"));  // NOLINT(cppcoreguidelines-owning-memory): владеет FilePtr
    if (!file_ || !push_to_disk()) {
        failed_ = true;
        return false;
    }
    return true;
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
