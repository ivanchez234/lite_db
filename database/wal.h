#pragma once
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include "../storage/metrics.h"
#include <memory>
#include <mutex>
#include <string>
#include <vector>


class Wal {
public:
    enum class Sync {
        none,
        flush, 
        full    
    };

    explicit Wal(std::string path, Sync mode = Sync::full);
    ~Wal();

    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    bool is_open() const;
    //флаг который показывает можно ли доверять .wal 
    bool healthy() const;

    Sync sync_mode() const;
    void set_sync_mode(Sync mode);


    [[nodiscard]] bool append(const std::string& payload);


    std::vector<std::string> replay() const;

    [[nodiscard]] bool reset();

    uint64_t appends() const noexcept { return appends_.get(); }
    uint64_t fsyncs() const noexcept { return fsyncs_.get(); }
    uint64_t bytes() const noexcept { return bytes_.get(); }

    static const char* sync_mode_name(Sync mode) noexcept;
    static bool parse_sync_mode(const std::string& text, Sync& out) noexcept;

private:
    struct FileCloser {

        void operator()(std::FILE* f) const noexcept {
            (void)std::fclose(f); // NOLINT(cppcoreguidelines-owning-memory): владеет unique_ptr
        }
    };
    using FilePtr = std::unique_ptr<std::FILE, FileCloser>;

    // Вызываются с захваченным mutex_.
    bool push_to_disk();
    bool write_record(const std::string& payload);

    std::string path_;
    mutable std::mutex mutex_; 
    Sync mode_;
    FilePtr file_;
    bool failed_ = false;

    // Групповой коммит: номера дописанных и подтверждённых fsync записей.
    uint64_t written_seq_ = 0;
    uint64_t  synced_seq_  = 0;
    bool syncing_ = false;  
    std::condition_variable synced_cv_;

    lite_db::Counter appends_;
    lite_db::Counter fsyncs_;
    lite_db::Counter bytes_;
};
