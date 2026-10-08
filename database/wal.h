#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

// Журнал предзаписи (write-ahead log).
//
// Запись попадает в журнал ДО того, как изменение применяется к данным,
// поэтому после аварии незавершённые операции можно воспроизвести.
//
// Формат одной записи: [uint32 crc32 данных][uint32 длина][данные].
// Контрольная сумма нужна не для борьбы с порчей диска, а чтобы отличить
// дописанную не до конца запись от целой: при аварии обрывается именно
// последняя запись, и воспроизводить её нельзя.
class Wal {
public:
    // Насколько далеко запись проталкивается на каждом вызове append().
    enum class Sync {
        none,   // только буфер процесса — не переживает даже его падения
        flush,  // отдаём ядру — переживает падение процесса, но не потерю питания
        full    // fsync — переживает и потерю питания, но заметно медленнее
    };

    explicit Wal(std::string path, Sync mode = Sync::full);
    ~Wal();

    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    bool is_open() const noexcept { return file_ != nullptr; }

    Sync sync_mode() const noexcept { return mode_; }
    void set_sync_mode(Sync mode) noexcept { mode_ = mode; }

    // Дописывает запись и проталкивает её согласно выбранному режиму.
    bool append(const std::string& payload);

    // Читает журнал. Останавливается на первой оборванной или повреждённой
    // записи: всё, что лежит после неё, доверия не заслуживает.
    std::vector<std::string> replay() const;

    // Данные дошли до диска, журнал больше не нужен — обрезаем его.
    void reset();

    static const char* sync_mode_name(Sync mode) noexcept;
    static bool parse_sync_mode(const std::string& text, Sync& out) noexcept;

private:
    bool push_to_disk();

    std::string path_;
    Sync        mode_;
    std::FILE*  file_ = nullptr;
};
