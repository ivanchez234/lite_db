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

// --- Данные и проверка по схеме ---------------------------------------------

TEST_CASE("строки с запятыми, кавычками и экранированием сохраняются как есть", "[database][regression]") {
    // Раньше "Smith, John" молча сохранялось как "Smith".
    reset_state();
    Database db(kWal);
    create_users(db);

    REQUIRE(db.execute(R"(INSERT use 1 {"name":"Smith, John","age":40,"is_active":true})") == "OK");
    REQUIRE(db.execute(R"(INSERT use 2 {"name":"say \"hi\" \\ {x}: y","age":1,"is_active":false})") == "OK");
    REQUIRE(db.execute("INSERT INTO use (id, name, age, is_active) VALUES (3, 'O''Brien, Pat', 2, 1)") == "OK");

    // Ответ — корректный JSON с экранированием.
    REQUIRE(db.execute("SELECT use 1") == R"({"name":"Smith, John","age":40,"is_active":true})");
    REQUIRE(db.execute("SELECT use 2") == R"({"name":"say \"hi\" \\ {x}: y","age":1,"is_active":false})");
    REQUIRE(db.execute("SELECT use 3 name") == "O'Brien, Pat");

    // И то же самое после сброса на диск и перезапуска.
    REQUIRE(db.execute("FLUSH").rfind("OK", 0) == 0);
    Database reopened(kWal);
    REQUIRE(reopened.execute("SELECT use 1") == R"({"name":"Smith, John","age":40,"is_active":true})");
    REQUIRE(reopened.execute("SELECT use 3 name") == "O'Brien, Pat");
}

TEST_CASE("типы проверяются строго", "[database]") {
    reset_state();
    Database db(kWal);
    create_users(db);

    const auto insert = [&](const std::string& body) { return db.execute("INSERT use 1 " + body); };

    // "30abc" раньше проходило как 30 (std::stoi читает начало строки).
    REQUIRE(insert(R"({"name":"A","age":"30","is_active":true})")    == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(insert(R"({"name":"A","age":30.5,"is_active":true})")    == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(insert(R"({"name":"A","age":99999999999,"is_active":true})") == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(insert(R"({"name":5,"age":30,"is_active":true})")        == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(insert(R"({"name":"A","age":30,"is_active":"yes"})")     == "ERR_CONSTRAINT_VIOLATION");
    // Лишнее поле раньше молча сохранялось и никогда не возвращалось.
    REQUIRE(insert(R"({"name":"A","age":30,"is_active":true,"x":1})") == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(insert(R"({"name":"A","age":30})")                       == "ERR_CONSTRAINT_VIOLATION");
    // Перевод строки разорвал бы построчный протокол.
    REQUIRE(insert(R"({"name":"line\nbreak","age":30,"is_active":true})") == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(insert(R"({"name":"A","age":30,"is_active":true)")       == "ERR_INVALID_JSON");
    REQUIRE(insert(R"(name=A)")                                      == "ERR_INVALID_JSON");

    REQUIRE(wal_records() == 0);   // отвергнутое в журнал не попало
    REQUIRE(insert(R"({"name":"A","age":-30,"is_active":0})") == "OK");
    REQUIRE(db.execute("SELECT use 1") == R"({"name":"A","age":-30,"is_active":false})");
}

TEST_CASE("DATE проверяется по календарю", "[database]") {
    reset_state();
    Database db(kWal);
    REQUIRE(db.execute("CREATE events").rfind("OK", 0) == 0);
    REQUIRE(db.execute("SCHEMA events title:STRING day:DATE") == "OK: Schema applied");

    REQUIRE(db.execute(R"(INSERT events 1 {"title":"a","day":"2024-02-29"})") == "OK");
    REQUIRE(db.execute(R"(INSERT events 2 {"title":"b","day":"2023-02-29"})") == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(db.execute(R"(INSERT events 3 {"title":"c","day":"2024-04-31"})") == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(db.execute(R"(INSERT events 4 {"title":"d","day":"2024-13-01"})") == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(db.execute(R"(INSERT events 5 {"title":"e","day":"1900-02-29"})") == "ERR_CONSTRAINT_VIOLATION");
    REQUIRE(db.execute(R"(INSERT events 6 {"title":"f","day":"2000-02-29"})") == "OK");
}

// --- UPDATE как в SQL ----------------------------------------------------------

TEST_CASE("UPDATE меняет только переданные поля", "[database]") {
    reset_state();
    Database db(kWal);
    create_users(db);

    REQUIRE(db.execute(R"(INSERT use 1 {"name":"Ivan","age":21,"is_active":true})") == "OK");
    REQUIRE(db.execute(R"(UPDATE use 1 {"age":22})") == "OK");
    REQUIRE(db.execute("SELECT use 1") == R"({"name":"Ivan","age":22,"is_active":true})");

    // SQL-форма — то же самое.
    REQUIRE(db.execute("UPDATE use SET is_active = 0 WHERE id = 1") == "OK");
    REQUIRE(db.execute("SELECT use 1") == R"({"name":"Ivan","age":22,"is_active":false})");

    // Поле берётся и с диска: сбрасываем и обновляем снова.
    REQUIRE(db.execute("FLUSH").rfind("OK", 0) == 0);
    REQUIRE(db.execute("UPDATE use SET name = 'Ivan, Jr' WHERE id = 1") == "OK");
    REQUIRE(db.execute("SELECT use 1") == R"({"name":"Ivan, Jr","age":22,"is_active":false})");

    // Несуществующую запись UPDATE не создаёт.
    REQUIRE(db.execute(R"(UPDATE use 99 {"age":1})") == "ERR_NOT_FOUND");
    REQUIRE(db.execute("SELECT use 99") == "ERR_NOT_FOUND");
    // Неверный тип в частичном обновлении тоже ловится.
    REQUIRE(db.execute(R"(UPDATE use 1 {"age":"много"})") == "ERR_CONSTRAINT_VIOLATION");
}

TEST_CASE("частичный UPDATE восстанавливается из журнала после аварии", "[database][recovery]") {
    reset_state();
    std::error_code ec;
    fs::remove_all("data_snapshot", ec);
    fs::remove("wal_snapshot.log", ec);

    {
        Database db(kWal);
        create_users(db);
        REQUIRE(db.execute(R"(INSERT use 1 {"name":"Ivan","age":21,"is_active":true})") == "OK");
        REQUIRE(db.execute("UPDATE use SET age = 30 WHERE id = 1") == "OK");
        fs::copy("data", "data_snapshot", fs::copy_options::recursive);
        fs::copy_file(kWal, "wal_snapshot.log");
    }

    fs::remove_all("data");
    fs::rename("data_snapshot", "data");
    fs::remove(kWal);
    fs::rename("wal_snapshot.log", kWal);

    Database recovered(kWal);
    recovered.recover_from_wal();
    REQUIRE(recovered.execute("SELECT use 1") == R"({"name":"Ivan","age":30,"is_active":true})");
}

// --- SQL, который мы не понимаем, отвергается -----------------------------------

TEST_CASE("непонятый SQL даёт ошибку, а не неверный результат", "[database][regression]") {
    reset_state();
    Database db(kWal);
    create_users(db);
    REQUIRE(db.execute(R"(INSERT use 1 {"name":"Ivan","age":21,"is_active":true})") == "OK");
    REQUIRE(db.execute(R"(INSERT use 2 {"name":"Maria","age":22,"is_active":true})") == "OK");

    // Раньше возвращалась вся таблица.
    REQUIRE(db.execute("SELECT * FROM use WHERE age = 22").rfind("ERR_SQL_PARSER", 0) == 0);
    // Раньше запись получала id = -1.
    REQUIRE(db.execute("INSERT INTO use (name, age, is_active) VALUES ('X', 1, 1)").rfind("ERR_SQL_PARSER", 0) == 0);
    REQUIRE(db.execute("SELECT use -1") == "ERR_NOT_FOUND");
    // Раньше DELETE с условием «age = 5 AND id = 1» удалял id 1, не глядя на age.
    REQUIRE(db.execute("DELETE FROM use WHERE age = 5 AND id = 1").rfind("ERR_SQL_PARSER", 0) == 0);
    REQUIRE(db.execute("SELECT use 1") == R"({"name":"Ivan","age":21,"is_active":true})");
}
