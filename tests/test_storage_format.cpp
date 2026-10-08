// Проверка формата хранения через публичный интерфейс Storage.
// Тесты намеренно идут через insert/select/flush, а не через приватные
// pack_columns/unpack_columns: так проверяется поведение, а не устройство.

#include <catch2/catch_test_macros.hpp>

#include "storage/storage.h"

#include <filesystem>
#include <string>

namespace {

// Каждый тест начинает с чистого каталога данных.
void reset_data_dir() {
    std::error_code ec;
    std::filesystem::remove_all("data", ec);
}

std::vector<Column> user_schema() {
    return {
        {"name",      DataType::STRING},
        {"age",       DataType::INT},
        {"is_active", DataType::BOOL}
    };
}

void flush_all(Storage& st) {
    REQUIRE(st.flush_all());
}

} // namespace

TEST_CASE("запись читается из буфера до сброса на диск", "[format]") {
    reset_data_dir();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", user_schema()));

    st.insert("use", 1, R"({"name":"Alex","age":30,"is_active":true})");

    REQUIRE(st.select("use", 1) == R"({"name":"Alex","age":30,"is_active":true})");
}

TEST_CASE("запись читается после сброса на диск", "[format]") {
    reset_data_dir();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", user_schema()));

    st.insert("use", 1, R"({"name":"Alex","age":30,"is_active":true})");
    st.insert("use", 2, R"({"name":"Maria","age":22,"is_active":false})");
    flush_all(st);

    REQUIRE(st.select("use", 1) == R"({"name":"Alex","age":30,"is_active":true})");
    REQUIRE(st.select("use", 2) == R"({"name":"Maria","age":22,"is_active":false})");
}

TEST_CASE("индекс восстанавливается при повторном открытии", "[format][regression]") {
    // Главный регресс: блоки писались zlib, а индекс при старте читал их LZ4,
    // поэтому после перезапуска сброшенные на диск записи не находились.
    reset_data_dir();

    {
        Storage st;
        REQUIRE(st.create_table("use"));
        REQUIRE(st.set_schema("use", user_schema()));

        for (int id = 1; id <= 50; ++id) {
            st.insert("use", id,
                      R"({"name":"Ivan","age":21,"is_active":true})");
        }
        flush_all(st);
    }

    Storage reopened;
    REQUIRE(reopened.select("use", 1)  == R"({"name":"Ivan","age":21,"is_active":true})");
    REQUIRE(reopened.select("use", 50) == R"({"name":"Ivan","age":21,"is_active":true})");
    REQUIRE(reopened.select("use", 51) == "ERR_NOT_FOUND");
}

TEST_CASE("select_all возвращает все записи с диска", "[format][regression]") {
    // Тот же баг с кодеками ломал и полное сканирование.
    reset_data_dir();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", user_schema()));

    st.insert("use", 1, R"({"name":"Alex","age":30,"is_active":true})");
    st.insert("use", 2, R"({"name":"Maria","age":22,"is_active":false})");
    flush_all(st);

    const std::string all = st.select_all("use");

    REQUIRE(all.find("\"id\": 1") != std::string::npos);
    REQUIRE(all.find("\"id\": 2") != std::string::npos);
    REQUIRE(all.find("Alex")  != std::string::npos);
    REQUIRE(all.find("Maria") != std::string::npos);
}

TEST_CASE("значения bool переживают сброс на диск", "[format][regression]") {
    // Значение bool выносится в битовую колонку и вырезается из записи.
    // Раньше оно определялось по первому символу, поэтому "false"
    // превращалось в true.
    reset_data_dir();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", user_schema()));

    st.insert("use", 1, R"({"name":"A","age":1,"is_active":true})");
    st.insert("use", 2, R"({"name":"B","age":2,"is_active":false})");
    st.insert("use", 3, R"({"name":"C","age":3,"is_active":1})");
    st.insert("use", 4, R"({"name":"D","age":4,"is_active":0})");
    flush_all(st);

    REQUIRE(st.select("use", 1) == R"({"name":"A","age":1,"is_active":true})");
    REQUIRE(st.select("use", 2) == R"({"name":"B","age":2,"is_active":false})");
    REQUIRE(st.select("use", 3) == R"({"name":"C","age":3,"is_active":true})");
    REQUIRE(st.select("use", 4) == R"({"name":"D","age":4,"is_active":false})");
}

TEST_CASE("чтение одного поля не требует разбора всей записи", "[format]") {
    reset_data_dir();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", user_schema()));

    st.insert("use", 1, R"({"name":"Alex","age":30,"is_active":true})");
    flush_all(st);

    REQUIRE(st.select("use", 1, "name") == "Alex");
    REQUIRE(st.select("use", 1, "age")  == "30");
    REQUIRE(st.select("use", 1, "nope") == "ERR_KEY_NOT_FOUND");
}

TEST_CASE("удалённая запись не находится", "[format]") {
    reset_data_dir();

    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", user_schema()));

    st.insert("use", 1, R"({"name":"Alex","age":30,"is_active":true})");
    flush_all(st);
    REQUIRE(st.exists("use", 1));

    st.remove("use", 1);
    flush_all(st);

    REQUIRE_FALSE(st.exists("use", 1));
    REQUIRE(st.select("use", 1) == "ERR_NOT_FOUND");
}

// --- Устойчивость к повреждённым данным ---------------------------------
//
// Длины и смещения читаются из самого файла. Эти тесты портят файл и
// проверяют, что чтение не уходит за пределы буфера, а возвращает ошибку.

namespace {

void fill_table_and_flush() {
    Storage st;
    REQUIRE(st.create_table("use"));
    REQUIRE(st.set_schema("use", user_schema()));
    for (int id = 1; id <= 20; ++id) {
        st.insert("use", id, R"({"name":"Ivan","age":21,"is_active":true})");
    }
    flush_all(st);
}

const std::filesystem::path kSegment = "data/use/seg_0.db";

} // namespace

TEST_CASE("обрезанный сегмент не ломает чтение", "[format][robustness]") {
    reset_data_dir();
    fill_table_and_flush();

    REQUIRE(std::filesystem::exists(kSegment));
    const auto full_size = std::filesystem::file_size(kSegment);
    REQUIRE(full_size > 16);

    // Рвём файл посередине блока.
    std::filesystem::resize_file(kSegment, full_size / 2);

    Storage reopened;                       // не должно падать
    const std::string one = reopened.select("use", 1);
    const std::string all = reopened.select_all("use");

    // Данных может не быть — важно, что ответ корректный, а не мусор.
    REQUIRE((one == "ERR_NOT_FOUND" || one.front() == '{'));
    REQUIRE((all == "[]" || all.front() == '['));
}

TEST_CASE("заголовок блока с огромными размерами отвергается", "[format][robustness]") {
    reset_data_dir();
    fill_table_and_flush();

    // CompressedBlockHeader — это два uint32 в начале блока.
    // Подставляем заведомо невозможные размеры.
    {
        std::fstream f(kSegment, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE(f.is_open());
        const uint32_t huge = 0xFFFFFFFFu;
        f.seekp(0);
        f.write(reinterpret_cast<const char*>(&huge), sizeof(huge));
        f.write(reinterpret_cast<const char*>(&huge), sizeof(huge));
    }

    Storage reopened;                       // не должно падать и не должно
    const std::string one = reopened.select("use", 1);   // выделять 4 ГБ

    REQUIRE(one == "ERR_NOT_FOUND");
}

TEST_CASE("мусор вместо тела блока не приводит к выходу за буфер", "[format][robustness]") {
    reset_data_dir();
    fill_table_and_flush();

    const auto full_size = std::filesystem::file_size(kSegment);
    {
        // Оставляем заголовок нетронутым, портим сжатые данные.
        std::fstream f(kSegment, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE(f.is_open());
        f.seekp(static_cast<std::streamoff>(sizeof(uint32_t) * 2));
        const std::string junk(static_cast<size_t>(full_size) / 2, '\xA5');
        f.write(junk.data(), static_cast<std::streamsize>(junk.size()));
    }

    Storage reopened;
    const std::string one = reopened.select("use", 1);
    const std::string all = reopened.select_all("use");

    REQUIRE((one == "ERR_NOT_FOUND" || one.front() == '{'));
    REQUIRE((all == "[]" || all.front() == '['));
}
