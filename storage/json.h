#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Минимальный JSON для записей lite_db.
//
// Запись — плоский объект: {"имя": значение, ...}, где значение — строка,
// число, true или false. Вложенные объекты, массивы и null не поддерживаются:
// схема таблицы их не описывает.
//
// Раньше объект «разбирался» удалением всех кавычек и скобок и разрезанием
// по запятым, поэтому "Smith, John" молча превращалось в "Smith".

namespace lite_db::json {

struct Value {
    // Для строки — уже раскодированный текст (без кавычек и экранирования),
    // для числа и true/false — литерал как есть.
    std::string text;
    bool        is_string = false;
};

using Field  = std::pair<std::string, Value>;
using Fields = std::vector<Field>;

// Разбирает плоский JSON-объект. При ошибке возвращает false и пишет
// в error, что именно не так.
bool parse_object(std::string_view input, Fields& out, std::string& error);

// Разбирает объект в начале input и сообщает, сколько символов он занял.
// Нужен для команд с несколькими объектами подряд (MPUT).
bool parse_object_prefix(std::string_view input, size_t& consumed, Fields& out, std::string& error);

// Строка в виде JSON-литерала: в кавычках, с экранированием.
std::string quote(std::string_view text);

// Является ли текст числом по грамматике JSON (без NaN, Infinity, ведущих нулей).
bool is_number(std::string_view text) noexcept;

} // namespace lite_db::json
