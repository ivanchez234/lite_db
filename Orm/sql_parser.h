#pragma once
#include <string>
#include <vector>

// Перевод подмножества SQL во внутренние команды lite_db.
//
//   INSERT INTO t (id, a, b) VALUES (1, 'x', 2)  ->  INSERT t 1 {"a":"x","b":2}
//   SELECT * FROM t WHERE id = 1                 ->  SELECT t 1
//   SELECT * FROM t                              ->  SELECT t ALL
//   UPDATE t SET a = 'y' WHERE id = 1            ->  UPDATE t 1 {"a":"y"}
//   DELETE FROM t WHERE id = 1                   ->  DELETE t 1
//
// Условие WHERE поддерживается только в виде «id = число». Всё, что
// транслятор не понимает, отвергается с ошибкой ERR_SQL_PARSER, а не
// выполняется как-нибудь: раньше WHERE по другой колонке молча
// игнорировался и SELECT возвращал всю таблицу.
//
// Структуру запроса разбирают регулярные выражения, а списки значений —
// ручной разбор с учётом кавычек, иначе запятая внутри 'Smith, John'
// резала значение пополам.

namespace SQLParser {
    // Переводит SQL в команду lite_db. Строки, не похожие на SQL
    // (CREATE, SCHEMA, INSERT t ..., SELECT t 1 и т. п.), возвращает как есть.
    std::string translate(const std::string& query);

    std::string trim(const std::string& str);

    // Делит список по разделителю, не заглядывая внутрь строк в одинарных
    // кавычках. false — кавычка не закрыта.
    bool split_list(const std::string& str, char delim, std::vector<std::string>& out);
}
