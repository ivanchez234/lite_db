// Разбор плоского JSON-объекта записи.

#include <catch2/catch_test_macros.hpp>

#include "storage/json.h"

#include <string>

using lite_db::json::Fields;
using lite_db::json::parse_object;
using lite_db::json::quote;

namespace {

Fields parse_ok(const std::string& text) {
    Fields fields;
    std::string error;
    INFO(text);
    REQUIRE(parse_object(text, fields, error));
    return fields;
}

bool parse_fails(const std::string& text) {
    Fields fields;
    std::string error;
    const bool ok = parse_object(text, fields, error);
    if (!ok) REQUIRE_FALSE(error.empty());  // ошибка всегда объясняется
    return !ok;
}

} // namespace

TEST_CASE("json: строки, числа и логические значения", "[json]") {
    const Fields f = parse_ok(R"({"name": "Ivan", "age": 21, "score": -1.5e3, "ok": true, "no": false})");
    REQUIRE(f.size() == 5);
    REQUIRE(f[0].first == "name");
    REQUIRE(f[0].second.text == "Ivan");
    REQUIRE(f[0].second.is_string);
    REQUIRE(f[1].second.text == "21");
    REQUIRE_FALSE(f[1].second.is_string);
    REQUIRE(f[2].second.text == "-1.5e3");
    REQUIRE(f[3].second.text == "true");
    REQUIRE(f[4].second.text == "false");
}

TEST_CASE("json: запятые, двоеточия и скобки внутри строки — часть значения", "[json][regression]") {
    // Раньше объект резался по запятым: "Smith, John" становилось "Smith".
    const Fields f = parse_ok(R"({"name": "Smith, John", "note": "a:b {x} [y]"})");
    REQUIRE(f[0].second.text == "Smith, John");
    REQUIRE(f[1].second.text == "a:b {x} [y]");
}

TEST_CASE("json: экранирование раскодируется", "[json]") {
    const Fields f = parse_ok(R"({"s": "say \"hi\" \\ \/ \t", "u": "Жé", "e": "😀"})");
    REQUIRE(f[0].second.text == "say \"hi\" \\ / \t");
    REQUIRE(f[1].second.text == "\xD0\x96\xC3\xA9");          // Жé в UTF-8
    REQUIRE(f[2].second.text == "\xF0\x9F\x98\x80");          // эмодзи из суррогатной пары
}

TEST_CASE("json: пустой объект и пробелы", "[json]") {
    REQUIRE(parse_ok("  { }  ").empty());
}

TEST_CASE("json: некорректный ввод отвергается с объяснением", "[json]") {
    REQUIRE(parse_fails(""));
    REQUIRE(parse_fails("{"));
    REQUIRE(parse_fails(R"({"a": 1,})"));
    REQUIRE(parse_fails(R"({"a" 1})"));
    REQUIRE(parse_fails(R"({a: 1})"));
    REQUIRE(parse_fails(R"({"a": "не закрыта})"));
    REQUIRE(parse_fails(R"({"a": 01})"));
    REQUIRE(parse_fails(R"({"a": 1.})"));
    REQUIRE(parse_fails(R"({"a": null})"));
    REQUIRE(parse_fails(R"({"a": {"b": 1}})"));
    REQUIRE(parse_fails(R"({"a": [1]})"));
    REQUIRE(parse_fails(R"({"a": 1, "a": 2})"));
    REQUIRE(parse_fails(R"({"a": 1} лишнее)"));
    REQUIRE(parse_fails(R"({"a": "\x"})"));
    REQUIRE(parse_fails(R"({"a": "\ud83d"})"));
    REQUIRE(parse_fails("{\"a\": \"raw\nline\"}"));   // управляющий символ без экранирования
}

TEST_CASE("json: quote экранирует так, что разбор вернёт исходное", "[json]") {
    const std::string tricky = "кавычка \" слеш \\ таб \t перевод \n и \x01";
    const std::string doc = "{\"v\": " + quote(tricky) + "}";
    const Fields f = parse_ok(doc);
    REQUIRE(f[0].second.text == tricky);
}
