#include "sql_parser.h"

#include "../storage/json.h"

#include <algorithm>
#include <cctype>
#include <regex>

// Почему здесь почти нет std::regex.
//
// Реализация std::regex в libstdc++ рекурсивна: глубина стека растёт с длиной
// входной строки. Запрос `INSERT ... VALUES (1, 'xxxx...')` в несколько
// килобайт переполнял стек потока (на Windows он по умолчанию 1 МБ) и ронял
// весь сервер — любой клиент мог положить базу одной строкой.
//
// Поэтому структура запроса (ключевые слова, скобки, списки, строки в
// кавычках) разбирается ручным проходом за линейное время, а регулярные
// выражения применяются только к коротким кускам с проверкой длины.

namespace SQLParser {

namespace {

// Регулярное выражение запускается только на строке не длиннее этого.
constexpr size_t kMaxRegexInput = 128;

std::string parse_error(const std::string& message) {
    return "ERR_SQL_PARSER: " + message;
}

bool is_space(char c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

bool is_word_char(char c) noexcept {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_';
}

std::string to_upper(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return text;
}

void skip_spaces(const std::string& s, size_t& pos) noexcept {
    while (pos < s.size() && is_space(s[pos])) ++pos;
}

// Читает слово из букв, цифр и '_'. false — слова в этой позиции нет.
bool read_word(const std::string& s, size_t& pos, std::string& out) {
    skip_spaces(s, pos);
    const size_t start = pos;
    while (pos < s.size() && is_word_char(s[pos])) ++pos;
    out = s.substr(start, pos - start);
    return !out.empty();
}

// Следующее слово совпадает с keyword (без учёта регистра).
bool read_keyword(const std::string& s, size_t& pos, const char* keyword) {
    size_t probe = pos;
    std::string word;
    if (!read_word(s, probe, word) || to_upper(word) != keyword) return false;
    pos = probe;
    return true;
}

bool is_identifier(const std::string& text) {
    return !text.empty() && std::all_of(text.begin(), text.end(), is_word_char);
}

bool is_integer(const std::string& text) {
    size_t i = (!text.empty() && text[0] == '-') ? 1 : 0;
    if (i >= text.size()) return false;
    for (; i < text.size(); ++i) {
        if (text[i] < '0' || text[i] > '9') return false;
    }
    return true;
}

// Позиция ключевого слова вне строк в кавычках (отдельным словом).
// last = true — последнее вхождение, иначе первое. npos — не найдено.
size_t find_keyword(const std::string& s, const std::string& keyword, size_t from, bool last) {
    const std::string upper = to_upper(s);
    size_t found = std::string::npos;
    bool in_quotes = false;

    for (size_t i = from; i < s.size(); ++i) {
        if (s[i] == '\'') {
            in_quotes = !in_quotes;
            continue;
        }
        if (in_quotes) continue;
        if (upper.compare(i, keyword.size(), keyword) != 0) continue;

        const bool starts = (i == 0) || !is_word_char(s[i - 1]);
        const size_t end = i + keyword.size();
        const bool ends = (end == s.size()) || !is_word_char(s[end]);
        if (!starts || !ends) continue;

        found = i;
        if (!last) break;
    }
    return found;
}

// "use.name" -> "name": SQLAlchemy пишет колонки с именем таблицы.
std::string strip_qualifier(const std::string& column) {
    const size_t dot = column.rfind('.');
    return dot == std::string::npos ? column : column.substr(dot + 1);
}

// Литерал SQL -> значение JSON.
//   'текст' (кавычка внутри удваивается: 'O''Brien')  -> "текст"
//   число                                              -> число
//   TRUE / FALSE                                       -> true / false
bool literal_to_json(const std::string& raw, std::string& out, std::string& error) {
    const std::string lit = trim(raw);
    if (lit.empty()) {
        error = "empty value";
        return false;
    }

    if (lit.front() == '\'') {
        if (lit.size() < 2 || lit.back() != '\'') {
            error = "unterminated string literal";
            return false;
        }
        std::string text;
        for (size_t i = 1; i + 1 < lit.size(); ++i) {
            if (lit[i] != '\'') {
                text += lit[i];
                continue;
            }
            // Внутри литерала кавычка допустима только удвоенной.
            if (i + 2 < lit.size() && lit[i + 1] == '\'') {
                text += '\'';
                ++i;
                continue;
            }
            error = "unescaped quote in string literal";
            return false;
        }
        out = lite_db::json::quote(text);
        return true;
    }

    const std::string upper = to_upper(lit);
    if (upper == "TRUE")  { out = "true";  return true; }
    if (upper == "FALSE") { out = "false"; return true; }
    if (upper == "NULL")  { error = "NULL is not supported"; return false; }
    if (lite_db::json::is_number(lit)) { out = lit; return true; }

    error = "unsupported value: " + lit.substr(0, 40);
    return false;
}

// "id = 5", "use.id = 5" -> "5". Любое другое условие — ошибка.
bool parse_id_condition(const std::string& condition, std::string& id) {
    const std::string cond = trim(condition);
    if (cond.size() > kMaxRegexInput) return false;

    static const std::regex re(R"(^(?:\w+\.)?id\s*=\s*(-?\d+)$)", std::regex::icase);
    std::smatch match;
    if (!std::regex_match(cond, match, re)) return false;
    id = match[1].str();
    return true;
}

const char* const kOnlyIdCondition = "only 'WHERE id = <number>' is supported";

// Список «колонка = значение» или значений -> тело JSON.
// names пуст — значит, values уже содержит пары «колонка = значение».
std::string build_json(const std::vector<std::string>& names,
                       const std::vector<std::string>& values,
                       std::string& error) {
    std::string json = "{";
    for (size_t i = 0; i < values.size(); ++i) {
        std::string value;
        if (!literal_to_json(values[i], value, error)) return "";
        if (i > 0) json += ',';
        json += lite_db::json::quote(names[i]);
        json += ':';
        json += value;
    }
    json += '}';
    return json;
}

// INSERT INTO t (id, a, b) VALUES (1, 'x', 2)
std::string translate_insert(const std::string& query, size_t pos) {
    std::string table;
    if (!read_word(query, pos, table)) return parse_error("expected table name");

    skip_spaces(query, pos);
    if (pos >= query.size() || query[pos] != '(') return parse_error("expected '(' with column list");
    const size_t columns_end = query.find(')', pos);
    if (columns_end == std::string::npos) return parse_error("unclosed column list");
    const std::string columns_text = query.substr(pos + 1, columns_end - pos - 1);
    pos = columns_end + 1;

    if (!read_keyword(query, pos, "VALUES")) return parse_error("expected VALUES");
    skip_spaces(query, pos);
    if (pos >= query.size() || query[pos] != '(' || query.back() != ')') {
        return parse_error("expected VALUES (...)");
    }
    const std::string values_text = query.substr(pos + 1, query.size() - pos - 2);

    std::vector<std::string> columns;
    std::vector<std::string> values;
    if (!split_list(columns_text, ',', columns) || !split_list(values_text, ',', values)) {
        return parse_error("unterminated string literal");
    }
    if (columns.size() != values.size()) return parse_error("columns and values mismatch");

    std::string id;
    std::vector<std::string> names;
    std::vector<std::string> field_values;

    for (size_t i = 0; i < columns.size(); ++i) {
        const std::string column = strip_qualifier(columns[i]);
        if (!is_identifier(column)) return parse_error("bad column name");

        if (to_upper(column) == "ID") {
            if (!is_integer(values[i])) return parse_error("id must be an integer");
            id = values[i];
            continue;  // id — ключ записи, а не поле внутри неё
        }
        names.push_back(column);
        field_values.push_back(values[i]);
    }

    // Раньше без колонки id запись молча получала id = -1.
    if (id.empty()) return parse_error("INSERT requires an id column");

    std::string error;
    const std::string json = build_json(names, field_values, error);
    if (!error.empty()) return parse_error(error);
    return "INSERT " + table + " " + id + " " + json;
}

// SELECT ... FROM t [WHERE id = n] [LIMIT n] [OFFSET n]
std::string translate_select(const std::string& query, size_t from_pos) {
    size_t pos = from_pos + 4;  // после FROM
    std::string table;
    if (!read_word(query, pos, table)) return parse_error("expected table name after FROM");

    const std::string tail = trim(query.substr(pos));
    if (tail.empty()) return "SELECT " + table + " ALL";

    // Корректный хвост короткий; длинный — заведомо не то, что мы понимаем.
    if (tail.size() > kMaxRegexInput) return parse_error("unsupported SELECT clause");

    static const std::regex tail_re(
        R"(^(?:WHERE\s+(.+?))?\s*(?:\bLIMIT\s+(\d+))?\s*(?:\bOFFSET\s+(\d+))?$)", std::regex::icase);
    std::smatch parts;
    if (!std::regex_match(tail, parts, tail_re)) {
        return parse_error("unsupported SELECT clause (only WHERE id = <n>, LIMIT, OFFSET)");
    }

    const bool has_where  = parts[1].matched;
    const bool has_limit  = parts[2].matched;
    const bool has_offset = parts[3].matched;

    if (!has_where) {
        // Полное чтение отдаёт всю таблицу. Молча проигнорировать LIMIT
        // значило бы вернуть не то, что просили.
        if (has_limit || has_offset) return parse_error("LIMIT/OFFSET are supported only with WHERE id = <n>");
        return "SELECT " + table + " ALL";
    }

    std::string id;
    if (!parse_id_condition(parts[1].str(), id)) return parse_error(kOnlyIdCondition);

    // Поиск по id даёт не больше одной строки: LIMIT >= 1 и OFFSET 0 ничего
    // не меняют (так пишет SQLAlchemy для .first()), другие значения — меняют.
    if (has_limit && parts[2].str().find_first_not_of('0') == std::string::npos) {
        return parse_error("LIMIT 0 is not supported");
    }
    if (has_offset && parts[3].str().find_first_not_of('0') != std::string::npos) {
        return parse_error("OFFSET other than 0 is not supported");
    }
    return "SELECT " + table + " " + id;
}

// UPDATE t SET a = 'x', b = 2 WHERE id = n
std::string translate_update(const std::string& query, const std::string& table, size_t pos) {
    // Последнее WHERE вне кавычек: строка в SET может сама содержать это слово.
    const size_t where = find_keyword(query, "WHERE", pos, true);
    if (where == std::string::npos) return parse_error("UPDATE requires WHERE id = <number>");

    std::string id;
    if (!parse_id_condition(query.substr(where + 5), id)) return parse_error(kOnlyIdCondition);

    std::vector<std::string> assignments;
    if (!split_list(query.substr(pos, where - pos), ',', assignments)) {
        return parse_error("unterminated string literal");
    }

    std::vector<std::string> names;
    std::vector<std::string> values;
    for (const std::string& assignment : assignments) {
        // Имя колонки знака '=' содержать не может, поэтому первый '=' — разделитель.
        const size_t eq = assignment.find('=');
        if (eq == std::string::npos) return parse_error("bad assignment in SET");

        const std::string column = strip_qualifier(trim(assignment.substr(0, eq)));
        if (!is_identifier(column)) return parse_error("bad column name");
        if (to_upper(column) == "ID") return parse_error("id cannot be changed");

        names.push_back(column);
        values.push_back(assignment.substr(eq + 1));
    }

    std::string error;
    const std::string json = build_json(names, values, error);
    if (!error.empty()) return parse_error(error);
    return "UPDATE " + table + " " + id + " " + json;
}

// DELETE FROM t WHERE id = n
std::string translate_delete(const std::string& query, size_t pos) {
    std::string table;
    if (!read_word(query, pos, table)) return parse_error("expected table name");

    if (!read_keyword(query, pos, "WHERE")) {
        // Удалить всю таблицу одной опечаткой — последнее, чего хочется.
        return parse_error("DELETE requires WHERE id = <number>");
    }

    std::string id;
    if (!parse_id_condition(query.substr(pos), id)) return parse_error(kOnlyIdCondition);
    return "DELETE " + table + " " + id;
}

} // namespace

std::string trim(const std::string& str) {
    const size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    const size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, last - first + 1);
}

bool split_list(const std::string& str, char delim, std::vector<std::string>& out) {
    out.clear();
    std::string current;
    bool in_quotes = false;

    for (const char c : str) {
        // Удвоенная кавычка внутри строки дважды переключит флаг — то, что нужно.
        if (c == '\'') in_quotes = !in_quotes;
        if (c == delim && !in_quotes) {
            out.push_back(trim(current));
            current.clear();
            continue;
        }
        current += c;
    }
    out.push_back(trim(current));
    return !in_quotes;
}

std::string translate(const std::string& raw_query) {
    std::string query = trim(raw_query);
    // Точка с запятой в конце — обычное окончание SQL-запроса.
    while (!query.empty() && query.back() == ';') query = trim(query.substr(0, query.size() - 1));
    if (query.empty()) return "";

    try {
        size_t pos = 0;
        std::string word;
        if (!read_word(query, pos, word)) return query;
        const std::string first = to_upper(word);

        // INSERT INTO ...   (родная команда: INSERT t id {...})
        if (first == "INSERT" && read_keyword(query, pos, "INTO")) {
            return translate_insert(query, pos);
        }

        // DELETE FROM ...   (родная команда: DELETE t id)
        if (first == "DELETE" && read_keyword(query, pos, "FROM")) {
            return translate_delete(query, pos);
        }

        // UPDATE t SET ...  (родная команда: UPDATE t id {...})
        if (first == "UPDATE") {
            std::string table;
            size_t after_table = pos;
            if (read_word(query, after_table, table) && read_keyword(query, after_table, "SET")) {
                return translate_update(query, table, after_table);
            }
        }

        // SELECT ... FROM t ...   (родная команда: SELECT t id | SELECT t ALL)
        if (first == "SELECT") {
            const size_t from = find_keyword(query, "FROM", pos, false);
            if (from != std::string::npos) return translate_select(query, from);
        }
    } catch (const std::exception& e) {
        return parse_error(e.what());
    }

    // Не SQL (CREATE, SCHEMA, родные команды) — как есть.
    return query;
}

} // namespace SQLParser
