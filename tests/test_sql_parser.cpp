// Перевод SQL во внутренние команды.

#include <catch2/catch_test_macros.hpp>

#include "Orm/sql_parser.h"

#include <string>

using SQLParser::translate;

namespace {

bool is_parse_error(const std::string& result) {
    return result.rfind("ERR_SQL_PARSER", 0) == 0;
}

} // namespace

TEST_CASE("sql: INSERT превращается в команду с JSON без id внутри", "[sql]") {
    REQUIRE(translate("INSERT INTO use (id, name, age, is_active) VALUES (1, 'Ivan', 21, 1)")
            == R"(INSERT use 1 {"name":"Ivan","age":21,"is_active":1})");
    REQUIRE(translate("insert into use (ID, name) values (-7, 'a');")
            == R"(INSERT use -7 {"name":"a"})");
}

TEST_CASE("sql: запятые и кавычки внутри строк", "[sql][regression]") {
    // Раньше VALUES резались по каждой запятой: 'Smith, John' давало два значения.
    REQUIRE(translate("INSERT INTO use (id, name) VALUES (1, 'Smith, John')")
            == R"(INSERT use 1 {"name":"Smith, John"})");
    // Кавычка внутри строки удваивается (стандарт SQL), в JSON — экранируется.
    REQUIRE(translate("INSERT INTO use (id, name) VALUES (2, 'O''Brien \"Jr\"')")
            == R"(INSERT use 2 {"name":"O'Brien \"Jr\""})");
    // Скобка внутри строки не закрывает VALUES.
    REQUIRE(translate("INSERT INTO use (id, name) VALUES (3, 'a) b')")
            == R"(INSERT use 3 {"name":"a) b"})");
}

TEST_CASE("sql: INSERT без id — ошибка, а не id = -1", "[sql][regression]") {
    REQUIRE(is_parse_error(translate("INSERT INTO use (name) VALUES ('x')")));
    REQUIRE(is_parse_error(translate("INSERT INTO use (id, name) VALUES ('1', 'x')")));
    REQUIRE(is_parse_error(translate("INSERT INTO use (id, name) VALUES (1)")));
    REQUIRE(is_parse_error(translate("INSERT INTO use (id, name) VALUES (1, 'x)")));
    REQUIRE(is_parse_error(translate("INSERT INTO use (id, name) VALUES (1, NULL)")));
}

TEST_CASE("sql: SELECT по id, с LIMIT/OFFSET от SQLAlchemy и без условия", "[sql]") {
    REQUIRE(translate("SELECT * FROM use WHERE id = 5") == "SELECT use 5");
    REQUIRE(translate("SELECT use.id AS use_id, use.name AS use_name FROM use "
                      "WHERE use.id = 888 LIMIT 1 OFFSET 0") == "SELECT use 888");
    REQUIRE(translate("select * from use") == "SELECT use ALL");
}

TEST_CASE("sql: WHERE не по id — ошибка, а не вся таблица", "[sql][regression]") {
    // Раньше условие молча игнорировалось и возвращались все строки.
    REQUIRE(is_parse_error(translate("SELECT * FROM use WHERE age = 22")));
    REQUIRE(is_parse_error(translate("SELECT * FROM use WHERE id = 1 AND age = 2")));
    REQUIRE(is_parse_error(translate("SELECT * FROM use ORDER BY id")));
    REQUIRE(is_parse_error(translate("SELECT * FROM use LIMIT 5")));
    REQUIRE(is_parse_error(translate("SELECT * FROM use WHERE id = 1 OFFSET 1")));
    REQUIRE(is_parse_error(translate("DELETE FROM use WHERE age = 5 AND id = 3")));
    REQUIRE(is_parse_error(translate("DELETE FROM use")));
}

TEST_CASE("sql: UPDATE передаёт только изменённые поля", "[sql]") {
    REQUIRE(translate("UPDATE use SET age = 22 WHERE id = 1") == R"(UPDATE use 1 {"age":22})");
    REQUIRE(translate("UPDATE use SET name = 'a = b, WHERE c', age = 3 WHERE use.id = 4")
            == R"(UPDATE use 4 {"name":"a = b, WHERE c","age":3})");
    REQUIRE(is_parse_error(translate("UPDATE use SET age = 1")));
    REQUIRE(is_parse_error(translate("UPDATE use SET id = 2 WHERE id = 1")));
}

TEST_CASE("sql: DELETE по id", "[sql]") {
    REQUIRE(translate("DELETE FROM use WHERE id = 3") == "DELETE use 3");
    REQUIRE(translate("DELETE FROM use WHERE use.id = 3;") == "DELETE use 3");
}

TEST_CASE("sql: родные команды проходят без изменений", "[sql]") {
    REQUIRE(translate("SELECT use 1") == "SELECT use 1");
    REQUIRE(translate("SELECT use ALL") == "SELECT use ALL");
    REQUIRE(translate("INSERT use 1 {\"a\": 1}") == "INSERT use 1 {\"a\": 1}");
    REQUIRE(translate("UPDATE use 1 {\"a\": 1}") == "UPDATE use 1 {\"a\": 1}");
    REQUIRE(translate("DELETE use 1") == "DELETE use 1");
    REQUIRE(translate("CREATE users") == "CREATE users");
    REQUIRE(translate("FLUSH") == "FLUSH");
}

TEST_CASE("sql: очень длинный запрос не роняет процесс", "[sql][regression]") {
    // std::regex в libstdc++ рекурсивен: строка в ~100 КБ (а при стеке 1 МБ,
    // как у потоков Windows, — уже в ~4 КБ) переполняла стек и роняла сервер.
    const std::string big(1u << 20, 'x');
    REQUIRE(translate("INSERT INTO use (id, name) VALUES (1, '" + big + "')")
            == "INSERT use 1 {\"name\":\"" + big + "\"}");
    REQUIRE(translate("SELECT " + big + " FROM use WHERE id = 1") == "SELECT use 1");
    REQUIRE(is_parse_error(translate("SELECT * FROM use WHERE " + big)));
    REQUIRE(translate("UPDATE use SET name = '" + big + "' WHERE id = 1")
            == "UPDATE use 1 {\"name\":\"" + big + "\"}");
}
