// Проверка параллельного доступа.
//
// Эти тесты имеет смысл запускать под ThreadSanitizer: без него гонка
// проявляется редко и недетерминированно, а санитайзер видит её сразу.
//   cmake -S . -B build-tsan -DCMAKE_CXX_FLAGS="-fsanitize=thread -g"

#include <catch2/catch_test_macros.hpp>

#include "storage/storage.h"

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace {

const std::string kRecord = R"({"name":"Ivan","age":21,"is_active":true})";

void reset_data() {
    std::error_code ec;
    std::filesystem::remove_all("data", ec);
}

std::vector<Column> schema() {
    return {
        {"name",      DataType::STRING},
        {"age",       DataType::INT},
        {"is_active", DataType::BOOL}
    };
}

} // namespace

TEST_CASE("параллельные чтения возвращают одно и то же", "[concurrency]") {
    reset_data();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", schema()));

    constexpr int kRecords = 100;
    for (int id = 1; id <= kRecords; ++id) st.insert("use", id, kRecord);
    REQUIRE(st.flush_all());

    std::atomic<int> mismatches{0};
    std::vector<std::thread> readers;

    for (int worker = 0; worker < 8; ++worker) {
        readers.emplace_back([&] {
            for (int n = 0; n < 200; ++n) {
                const int id = 1 + (n % kRecords);
                if (st.select("use", id) != kRecord) mismatches.fetch_add(1);
            }
        });
    }
    for (auto& r : readers) r.join();

    REQUIRE(mismatches.load() == 0);
}

TEST_CASE("чтение во время записи не наблюдает полузаписанных данных", "[concurrency]") {
    reset_data();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", schema()));

    // Эти записи существуют с самого начала, их значение меняться не должно.
    constexpr int kStable = 50;
    for (int id = 1; id <= kStable; ++id) st.insert("use", id, kRecord);
    REQUIRE(st.flush_all());

    std::atomic<int> mismatches{0};

    // Число проходов ограничено намеренно. Читатели в бесконечном цикле
    // не дают писателю получить эксклюзивный замок: std::shared_mutex
    // в libstdc++ отдаёт предпочтение читателям, и писатель голодает.
    constexpr int kReaderPasses = 200;

    std::vector<std::thread> readers;
    for (int worker = 0; worker < 4; ++worker) {
        readers.emplace_back([&] {
            for (int pass = 0; pass < kReaderPasses; ++pass) {
                for (int id = 1; id <= kStable; ++id) {
                    const std::string got = st.select("use", id);
                    if (got != kRecord) mismatches.fetch_add(1);
                }
            }
        });
    }

    // Пишущие потоки добавляют новые идентификаторы, не трогая существующие.
    std::vector<std::thread> writers;
    for (int worker = 0; worker < 2; ++worker) {
        writers.emplace_back([&, worker] {
            for (int n = 0; n < 150; ++n) {
                const int id = 1000 + worker * 1000 + n;
                st.insert("use", id, kRecord);
            }
        });
    }

    for (auto& w : writers) w.join();
    for (auto& r : readers) r.join();

    REQUIRE(mismatches.load() == 0);
    REQUIRE(st.select("use", 1000) == kRecord);
    REQUIRE(st.select("use", 2000) == kRecord);
}

TEST_CASE("удаление и чтение идут параллельно без порчи данных", "[concurrency]") {
    reset_data();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", schema()));

    constexpr int kRecords = 60;
    for (int id = 1; id <= kRecords; ++id) st.insert("use", id, kRecord);
    REQUIRE(st.flush_all());

    std::atomic<int> bad{0};

    std::thread remover([&] {
        for (int id = 1; id <= kRecords; id += 2) st.remove("use", id);
    });

    std::vector<std::thread> readers;
    for (int worker = 0; worker < 4; ++worker) {
        readers.emplace_back([&] {
            for (int pass = 0; pass < 50; ++pass) {
                for (int id = 2; id <= kRecords; id += 2) {
                    // Чётные идентификаторы не удаляются и обязаны читаться.
                    if (st.select("use", id) != kRecord) bad.fetch_add(1);
                }
            }
        });
    }

    remover.join();
    for (auto& r : readers) r.join();

    REQUIRE(bad.load() == 0);
}
