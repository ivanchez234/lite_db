#pragma once

#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
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
//
// Потокобезопасен: записи в разные таблицы приходят из разных потоков.
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

    bool is_open() const;

    // false после первой же неудачной записи — и до перезапуска процесса.
    //
    // После провала fsync ядро помечает страницы файла чистыми, и повторный
    // fsync вернёт успех, хотя данных на диске нет (история «fsyncgate»
    // в PostgreSQL). Значит, после ошибки состоянию файла верить нельзя,
    // и честнее отказывать в записи, чем делать вид, что всё сохранено.
    // При перезапуске журнал читается с диска, а не из кеша, — это и есть
    // восстановление.
    bool healthy() const;

    Sync sync_mode() const;
    void set_sync_mode(Sync mode);

    // Дописывает запись и проталкивает её согласно выбранному режиму.
    // false — запись не сохранена, операцию применять нельзя.
    [[nodiscard]] bool append(const std::string& payload);

    // Читает журнал. Останавливается на первой оборванной или повреждённой
    // записи: всё, что лежит после неё, доверия не заслуживает.
    std::vector<std::string> replay() const;

    // Данные дошли до диска, журнал больше не нужен — обрезаем его.
    [[nodiscard]] bool reset();

    static const char* sync_mode_name(Sync mode) noexcept;
    static bool parse_sync_mode(const std::string& text, Sync& out) noexcept;

private:
    struct FileCloser {
        // Ошибку fclose здесь сообщить некому; данные к этому моменту уже
        // проталкиваются push_to_disk(), который свои ошибки возвращает.
        void operator()(std::FILE* f) const noexcept {
            (void)std::fclose(f);  // NOLINT(cppcoreguidelines-owning-memory): владеет unique_ptr
        }
    };
    using FilePtr = std::unique_ptr<std::FILE, FileCloser>;

    // Вызываются с захваченным mutex_.
    bool push_to_disk();
    bool write_record(const std::string& payload);

    std::string        path_;
    mutable std::mutex mutex_;   // защищает всё ниже
    Sync               mode_;
    FilePtr            file_;
    bool               failed_ = false;
};
