// Проверка журнала предзаписи.
//
// Главное, что здесь проверяется, — поведение при аварии: журнал почти всегда
// обрывается на середине последней записи, и воспроизводить её нельзя.

#include <catch2/catch_test_macros.hpp>

#include "database/wal.h"

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace {

const std::string kPath = "test_wal.log";

void remove_log() {
    std::error_code ec;
    std::filesystem::remove(kPath, ec);
}

void truncate_log(std::uintmax_t bytes) {
    std::filesystem::resize_file(kPath, bytes);
}

std::uintmax_t log_size() {
    return std::filesystem::file_size(kPath);
}

} // namespace

TEST_CASE("записи возвращаются в том же порядке", "[wal]") {
    remove_log();

    {
        Wal wal(kPath, Wal::Sync::flush);
        REQUIRE(wal.is_open());
        REQUIRE(wal.append("INSERT use 1 {\"name\":\"Alex\"}"));
        REQUIRE(wal.append("INSERT use 2 {\"name\":\"Maria\"}"));
        REQUIRE(wal.append("DELETE use 1"));
    }

    Wal reopened(kPath, Wal::Sync::flush);
    const auto records = reopened.replay();

    REQUIRE(records.size() == 3);
    REQUIRE(records[0] == "INSERT use 1 {\"name\":\"Alex\"}");
    REQUIRE(records[1] == "INSERT use 2 {\"name\":\"Maria\"}");
    REQUIRE(records[2] == "DELETE use 1");
}

TEST_CASE("оборванная последняя запись отбрасывается", "[wal][regression]") {
    remove_log();

    {
        Wal wal(kPath, Wal::Sync::flush);
        REQUIRE(wal.append("INSERT use 1 {\"name\":\"Alex\"}"));
        REQUIRE(wal.append("INSERT use 2 {\"name\":\"Maria\"}"));
        REQUIRE(wal.append("INSERT use 3 {\"name\":\"Bob\"}"));
    }

    // Обрываем файл на середине последней записи — так выглядит падение
    // процесса посреди записи в журнал.
    truncate_log(log_size() - 10);

    Wal reopened(kPath, Wal::Sync::flush);
    const auto records = reopened.replay();

    REQUIRE(records.size() == 2);
    REQUIRE(records[1] == "INSERT use 2 {\"name\":\"Maria\"}");
}

TEST_CASE("обрыв внутри заголовка записи не ломает чтение", "[wal]") {
    remove_log();

    {
        Wal wal(kPath, Wal::Sync::flush);
        REQUIRE(wal.append("INSERT use 1 {\"name\":\"Alex\"}"));
        REQUIRE(wal.append("INSERT use 2 {\"name\":\"Maria\"}"));
    }

    // Оставляем от второй записи только пару байт заголовка.
    const auto first_record_end = 4 + 4 + std::string("INSERT use 1 {\"name\":\"Alex\"}").size();
    truncate_log(first_record_end + 3);

    Wal reopened(kPath, Wal::Sync::flush);
    const auto records = reopened.replay();

    REQUIRE(records.size() == 1);
    REQUIRE(records[0] == "INSERT use 1 {\"name\":\"Alex\"}");
}

TEST_CASE("повреждённая запись отсекается контрольной суммой", "[wal][regression]") {
    remove_log();

    {
        Wal wal(kPath, Wal::Sync::flush);
        REQUIRE(wal.append("INSERT use 1 {\"name\":\"Alex\"}"));
        REQUIRE(wal.append("INSERT use 2 {\"name\":\"Maria\"}"));
    }

    // Портим один байт в теле первой записи. Длина осталась правильной,
    // поэтому поймать подмену может только контрольная сумма.
    {
        std::fstream f(kPath, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE(f.is_open());
        f.seekp(4 + 4 + 2);
        const char junk = 'X';
        f.write(&junk, 1);
    }

    Wal reopened(kPath, Wal::Sync::flush);
    const auto records = reopened.replay();

    REQUIRE(records.empty());
}

TEST_CASE("reset очищает журнал", "[wal]") {
    remove_log();

    Wal wal(kPath, Wal::Sync::flush);
    REQUIRE(wal.append("INSERT use 1 {\"name\":\"Alex\"}"));
    REQUIRE(wal.replay().size() == 1);

    REQUIRE(wal.reset());
    REQUIRE(wal.replay().empty());

    // После очистки журнал остаётся рабочим.
    REQUIRE(wal.append("INSERT use 2 {\"name\":\"Maria\"}"));
    REQUIRE(wal.replay().size() == 1);
}

TEST_CASE("режимы надёжности разбираются из конфигурации", "[wal]") {
    Wal::Sync mode = Wal::Sync::none;

    REQUIRE(Wal::parse_sync_mode("full", mode));
    REQUIRE(mode == Wal::Sync::full);
    REQUIRE(Wal::parse_sync_mode("flush", mode));
    REQUIRE(mode == Wal::Sync::flush);
    REQUIRE(Wal::parse_sync_mode("none", mode));
    REQUIRE(mode == Wal::Sync::none);

    REQUIRE_FALSE(Wal::parse_sync_mode("sometimes", mode));
    REQUIRE(std::string(Wal::sync_mode_name(Wal::Sync::full)) == "full");
}

TEST_CASE("запись работает во всех режимах", "[wal]") {
    for (auto mode : {Wal::Sync::none, Wal::Sync::flush, Wal::Sync::full}) {
        remove_log();
        {
            Wal wal(kPath, mode);
            REQUIRE(wal.append("INSERT use 1 {\"name\":\"Alex\"}"));
        }
        Wal reopened(kPath, mode);
        REQUIRE(reopened.replay().size() == 1);
    }
}

TEST_CASE("групповой коммит: параллельные записи все сохраняются, fsync меньше записей", "[wal][concurrency]") {
    // Под ThreadSanitizer этот тест проверяет схему «лидер делает fsync
    // без мьютекса, остальные дописывают и ждут».
    remove_log();

    constexpr int kThreads = 8;
    constexpr int kPerThread = 100;
    uint64_t appends = 0;
    uint64_t fsyncs = 0;
    {
        Wal wal(kPath, Wal::Sync::full);
        REQUIRE(wal.is_open());

        std::atomic<int> failures{0};
        std::vector<std::thread> threads;
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&, t] {
                for (int i = 0; i < kPerThread; ++i) {
                    if (!wal.append("INSERT t " + std::to_string(t * 1000 + i) + " {}")) failures.fetch_add(1);
                }
            });
        }
        for (auto& th : threads) th.join();

        REQUIRE(failures.load() == 0);
        appends = wal.appends();
        fsyncs = wal.fsyncs();
    }

    REQUIRE(appends == kThreads * kPerThread);
    REQUIRE(fsyncs >= 1);
    REQUIRE(fsyncs <= appends);   // обычно заметно меньше — fsync делится между записями

    Wal reopened(kPath, Wal::Sync::flush);
    const auto records = reopened.replay();
    REQUIRE(records.size() == static_cast<size_t>(kThreads * kPerThread));

    // Внутри каждого потока порядок сохранён.
    std::vector<int> last(kThreads, -1);
    for (const auto& r : records) {
        const int id = std::stoi(r.substr(9, r.find(' ', 9) - 9));
        const int t = id / 1000;
        REQUIRE(id % 1000 > last[t]);
        last[t] = id % 1000;
    }
}
