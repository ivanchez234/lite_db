// Проверка Database: связка журнала, хранилища и замка контрольной точки.
//
// Главное здесь — инварианты надёжности:
//   * клиент получает OK только за то, что лежит в журнале;
//   * в журнал не попадает то, что было отвергнуто;
//   * после аварии всё подтверждённое восстанавливается из журнала.

#include <catch2/catch_test_macros.hpp>

#include "database/database.h"
#include "database/wal.h"

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

const std::string kWal = "test_db_wal.log";

void reset_state() {
    std::error_code ec;
    fs::remove_all("data", ec);
    fs::remove(kWal, ec);
}

void create_users(Database& db) {
    REQUIRE(db.execute("CREATE use").rfind("OK", 0) == 0);
    REQUIRE(db.execute("SCHEMA use name:STRING age:INT is_active:BOOL") == "OK: Schema applied");
}

// Сколько записей сейчас лежит в журнале.
size_t wal_records() {
    // Отдельный объект только читает файл; открытие в режиме дозаписи его не меняет.
    Wal reader(kWal, Wal::Sync::none);
    return reader.replay().size();
}

} // namespace

TEST_CASE("INSERT занятого id отклоняется и не попадает в журнал", "[database]") {
    reset_state();
    Database db(kWal);
    create_users(db);

    REQUIRE(db.execute(R"(INSERT use 1 {"name":"Alex","age":30,"is_active":true})") == "OK");
    REQUIRE(db.execute(R"(INSERT use 1 {"name":"Copy","age":1,"is_active":false})") == "ERR_ID_EXISTS");

    REQUIRE(wal_records() == 1);
    REQUIRE(db.execute("SELECT use 1") == R"({"name":"Alex","age":30,"is_active":true})");
}

TEST_CASE("данные, не прошедшие проверку схемы, не попадают в журнал", "[database]") {
    reset_state();
    Database db(kWal);
    create_users(db);

    REQUIRE(db.execute(R"(INSERT use 1 {"name":"Alex","age":"много","is_active":true})")
            == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(wal_records() == 0);
}

TEST_CASE("UPDATE и DELETE пишутся в журнал, DELETE несуществующего — нет", "[database]") {
    reset_state();
    Database db(kWal);
    create_users(db);

    REQUIRE(db.execute(R"(INSERT use 1 {"name":"Alex","age":30,"is_active":true})") == "OK");
    REQUIRE(db.execute(R"(UPDATE use 1 {"name":"Alex","age":31,"is_active":true})") == "OK");
    REQUIRE(db.execute("DELETE use 1") == "OK");
    REQUIRE(db.execute("DELETE use 1") == "ERR_NOT_FOUND");

    REQUIRE(wal_records() == 3);
    REQUIRE(db.execute("SELECT use 1") == "ERR_NOT_FOUND");
}

TEST_CASE("FLUSH сбрасывает данные на диск и очищает журнал", "[database]") {
    reset_state();
    {
        Database db(kWal);
        create_users(db);
        REQUIRE(db.execute(R"(INSERT use 1 {"name":"Alex","age":30,"is_active":true})") == "OK");
        REQUIRE(wal_records() == 1);

        REQUIRE(db.execute("FLUSH") == "OK: All buffers flushed to disk");
        REQUIRE(wal_records() == 0);
    }

    Database reopened(kWal);
    REQUIRE(reopened.execute("SELECT use 1") == R"({"name":"Alex","age":30,"is_active":true})");
}

TEST_CASE("после аварии подтверждённые записи восстанавливаются из журнала", "[database][recovery]") {
    // Авария моделируется так: снимаем копию всего, что лежит на диске,
    // пока база работает (данные ещё в буфере, а не в сегментах), даём базе
    // закрыться и подменяем файлы снимком. Получаем ровно то состояние диска,
    // которое осталось бы после внезапного завершения процесса.
    reset_state();
    std::error_code ec;
    fs::remove_all("data_snapshot", ec);
    fs::remove("wal_snapshot.log", ec);

    {
        Database db(kWal);
        create_users(db);
        for (int id = 1; id <= 5; ++id) {
            const std::string row = R"({"name":"User)" + std::to_string(id)
                                  + R"(","age":20,"is_active":true})";
            REQUIRE(db.execute("INSERT use " + std::to_string(id) + " " + row) == "OK");
        }
        REQUIRE(db.execute("DELETE use 3") == "OK");

        fs::copy("data", "data_snapshot", fs::copy_options::recursive);
        fs::copy_file(kWal, "wal_snapshot.log");
    }   // штатное закрытие — его результат мы сейчас выбросим

    fs::remove_all("data");
    fs::rename("data_snapshot", "data");
    fs::remove(kWal);
    fs::rename("wal_snapshot.log", kWal);

    // Убеждаемся, что модель честная: в сегментах этих данных нет.
    REQUIRE_FALSE(fs::exists("data/use/seg_0.db"));
    REQUIRE(wal_records() == 6);

    Database recovered(kWal);
    recovered.recover_from_wal();

    REQUIRE(recovered.execute("SELECT use 1") == R"({"name":"User1","age":20,"is_active":true})");
    REQUIRE(recovered.execute("SELECT use 5") == R"({"name":"User5","age":20,"is_active":true})");
    REQUIRE(recovered.execute("SELECT use 3") == "ERR_NOT_FOUND");
}

TEST_CASE("сбой журнала: клиент получает ошибку, данные не применяются", "[database]") {
    // Журнал по пути, который является каталогом, открыть нельзя —
    // так моделируется сломанный диск, не трогая систему.
    std::error_code ec;
    fs::remove_all("data", ec);
    const std::string brokenWal = "wal_is_a_directory";
    fs::remove_all(brokenWal, ec);
    fs::create_directory(brokenWal);

    Database db(brokenWal);
    create_users(db);

    REQUIRE(db.execute(R"(INSERT use 1 {"name":"Alex","age":30,"is_active":true})")
            == "ERR_WAL_WRITE_FAILED");
    REQUIRE(db.execute("SELECT use 1") == "ERR_NOT_FOUND");

    fs::remove_all(brokenWal, ec);
}

TEST_CASE("записи в разные таблицы и FLUSH идут параллельно без потерь", "[database][concurrency]") {
    // Записи берут замок контрольной точки разделяемо, FLUSH — эксклюзивно.
    // Под ThreadSanitizer этот тест проверяет, что такая схема не гоняется.
    reset_state();
    Database db(kWal);

    for (const char* table : {"left", "right"}) {
        REQUIRE(db.execute(std::string("CREATE ") + table).rfind("OK", 0) == 0);
        REQUIRE(db.execute(std::string("SCHEMA ") + table + " name:STRING age:INT")
                == "OK: Schema applied");
    }

    constexpr int kPerWriter = 150;
    std::atomic<int> failures{0};
    std::atomic<bool> writersDone{false};

    std::vector<std::thread> writers;
    for (int w = 0; w < 4; ++w) {
        writers.emplace_back([&, w] {
            const std::string table = (w % 2 == 0) ? "left" : "right";
            for (int n = 0; n < kPerWriter; ++n) {
                const int id = w * 10000 + n;
                const std::string cmd = "INSERT " + table + " " + std::to_string(id)
                                      + R"( {"name":"N","age":)" + std::to_string(n) + "}";
                if (db.execute(cmd) != "OK") failures.fetch_add(1);
            }
        });
    }

    std::thread flusher([&] {
        while (!writersDone.load()) {
            if (db.execute("FLUSH").rfind("OK", 0) != 0) failures.fetch_add(1);
            std::this_thread::yield();
        }
    });

    for (auto& w : writers) w.join();
    writersDone.store(true);
    flusher.join();

    REQUIRE(failures.load() == 0);

    for (int w = 0; w < 4; ++w) {
        const std::string table = (w % 2 == 0) ? "left" : "right";
        for (int n = 0; n < kPerWriter; n += 37) {
            const std::string got = db.execute("SELECT " + table + " " + std::to_string(w * 10000 + n));
            REQUIRE(got == R"({"name":"N","age":)" + std::to_string(n) + "}");
        }
    }
}
