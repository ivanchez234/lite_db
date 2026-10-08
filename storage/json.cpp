#include "json.h"

#include <cstdint>

namespace lite_db::json {

namespace {

bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }

// Последовательный разбор с позицией и текстом ошибки.
class Parser {
public:
    explicit Parser(std::string_view text) : s_(text) {}

    // whole = true: после объекта не должно быть ничего, кроме пробелов.
    bool object(Fields& out, bool whole) {
        skip_ws();
        if (!expect('{')) return false;
        skip_ws();

        if (peek() == '}') {
            ++pos_;
        } else {
            for (;;) {
                Field field;
                skip_ws();
                if (peek() != '"') return fail("ожидалось имя поля в кавычках");
                if (!string(field.first)) return false;

                skip_ws();
                if (!expect(':')) return false;
                skip_ws();
                if (!value(field.second)) return false;

                for (const Field& existing : out) {
                    if (existing.first == field.first) return fail("поле \"" + field.first + "\" повторяется");
                }
                out.push_back(std::move(field));

                skip_ws();
                if (peek() == ',') { ++pos_; continue; }
                if (peek() == '}') { ++pos_; break; }
                return fail("ожидалась ',' или '}'");
            }
        }

        if (!whole) return true;
        skip_ws();
        if (pos_ != s_.size()) return fail("лишние символы после объекта");
        return true;
    }

    const std::string& error() const { return error_; }
    size_t position() const noexcept { return pos_; }

private:
    char peek() const noexcept { return pos_ < s_.size() ? s_[pos_] : '\0'; }

    void skip_ws() noexcept {
        while (pos_ < s_.size() && (s_[pos_] == ' ' || s_[pos_] == '\t' ||
                                    s_[pos_] == '\n' || s_[pos_] == '\r')) {
            ++pos_;
        }
    }

    bool expect(char c) {
        if (peek() != c) return fail(std::string("ожидался символ '") + c + "'");
        ++pos_;
        return true;
    }

    bool fail(const std::string& message) {
        if (error_.empty()) error_ = message + " (позиция " + std::to_string(pos_) + ")";
        return false;
    }

    bool value(Value& out) {
        const char c = peek();
        if (c == '"') {
            out.is_string = true;
            return string(out.text);
        }
        if (c == '-' || is_digit(c)) return number(out.text);
        if (s_.compare(pos_, 4, "true") == 0)  { out.text = "true";  pos_ += 4; return true; }
        if (s_.compare(pos_, 5, "false") == 0) { out.text = "false"; pos_ += 5; return true; }
        if (s_.compare(pos_, 4, "null") == 0)  return fail("null не поддерживается");
        if (c == '{' || c == '[')              return fail("вложенные объекты и массивы не поддерживаются");
        return fail("ожидалось значение");
    }

    bool number(std::string& out) {
        const size_t start = pos_;
        if (peek() == '-') ++pos_;
        if (peek() == '0') {
            ++pos_;
        } else if (is_digit(peek())) {
            while (is_digit(peek())) ++pos_;
        } else {
            return fail("некорректное число");
        }
        if (peek() == '.') {
            ++pos_;
            if (!is_digit(peek())) return fail("некорректное число");
            while (is_digit(peek())) ++pos_;
        }
        if (peek() == 'e' || peek() == 'E') {
            ++pos_;
            if (peek() == '+' || peek() == '-') ++pos_;
            if (!is_digit(peek())) return fail("некорректное число");
            while (is_digit(peek())) ++pos_;
        }
        out.assign(s_.substr(start, pos_ - start));
        return true;
    }

    // Читает 4 шестнадцатеричные цифры после \u.
    bool hex4(uint32_t& code) {
        if (s_.size() - pos_ < 4) return fail("обрыв \\u-последовательности");
        code = 0;
        for (int i = 0; i < 4; ++i) {
            const char c = s_[pos_++];
            code <<= 4;
            if (c >= '0' && c <= '9')      code |= static_cast<uint32_t>(c - '0');
            else if (c >= 'a' && c <= 'f') code |= static_cast<uint32_t>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') code |= static_cast<uint32_t>(c - 'A' + 10);
            else return fail("некорректная \\u-последовательность");
        }
        return true;
    }

    static void append_utf8(std::string& out, uint32_t cp) {
        if (cp < 0x80) {
            out += static_cast<char>(cp);
        } else if (cp < 0x800) {
            out += static_cast<char>(0xC0 | (cp >> 6));
            out += static_cast<char>(0x80 | (cp & 0x3F));
        } else if (cp < 0x10000) {
            out += static_cast<char>(0xE0 | (cp >> 12));
            out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
            out += static_cast<char>(0x80 | (cp & 0x3F));
        } else {
            out += static_cast<char>(0xF0 | (cp >> 18));
            out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
            out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
            out += static_cast<char>(0x80 | (cp & 0x3F));
        }
    }

    bool string(std::string& out) {
        ++pos_;  // открывающая кавычка
        out.clear();

        while (pos_ < s_.size()) {
            const char c = s_[pos_++];
            if (c == '"') return true;
            if (static_cast<unsigned char>(c) < 0x20) return fail("управляющий символ внутри строки");
            if (c != '\\') {
                out += c;
                continue;
            }

            if (pos_ >= s_.size()) break;
            const char e = s_[pos_++];
            switch (e) {
                case '"':  out += '"';  break;
                case '\\': out += '\\'; break;
                case '/':  out += '/';  break;
                case 'b':  out += '\b'; break;
                case 'f':  out += '\f'; break;
                case 'n':  out += '\n'; break;
                case 'r':  out += '\r'; break;
                case 't':  out += '\t'; break;
                case 'u': {
                    uint32_t cp = 0;
                    if (!hex4(cp)) return false;
                    // Символы вне базовой плоскости приходят парой суррогатов.
                    if (cp >= 0xD800 && cp <= 0xDBFF) {
                        if (s_.compare(pos_, 2, "\\u") != 0) return fail("одиночный суррогат в \\u");
                        pos_ += 2;
                        uint32_t low = 0;
                        if (!hex4(low)) return false;
                        if (low < 0xDC00 || low > 0xDFFF) return fail("некорректная суррогатная пара");
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                    } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                        return fail("одиночный суррогат в \\u");
                    }
                    append_utf8(out, cp);
                    break;
                }
                default:
                    return fail("неизвестная escape-последовательность");
            }
        }
        return fail("строка не закрыта");
    }

    std::string_view s_;
    size_t           pos_ = 0;
    std::string      error_;
};

} // namespace

bool parse_object(std::string_view input, Fields& out, std::string& error) {
    out.clear();
    Parser parser(input);
    if (parser.object(out, true)) return true;
    error = parser.error();
    return false;
}

bool parse_object_prefix(std::string_view input, size_t& consumed, Fields& out, std::string& error) {
    out.clear();
    Parser parser(input);
    if (!parser.object(out, false)) {
        error = parser.error();
        return false;
    }
    consumed = parser.position();
    return true;
}

std::string quote(std::string_view text) {
    static const char* const kHex = "0123456789abcdef";

    std::string out;
    out.reserve(text.size() + 2);
    out += '"';
    for (const char c : text) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b";  break;
            case '\f': out += "\\f";  break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    out += "\\u00";
                    out += kHex[(static_cast<unsigned char>(c) >> 4) & 0xF];
                    out += kHex[static_cast<unsigned char>(c) & 0xF];
                } else {
                    out += c;  // UTF-8 передаётся как есть
                }
        }
    }
    out += '"';
    return out;
}

bool is_number(std::string_view text) noexcept {
    size_t i = 0;
    const size_t n = text.size();
    if (i < n && text[i] == '-') ++i;
    if (i < n && text[i] == '0') {
        ++i;
    } else if (i < n && is_digit(text[i])) {
        while (i < n && is_digit(text[i])) ++i;
    } else {
        return false;
    }
    if (i < n && text[i] == '.') {
        ++i;
        if (i >= n || !is_digit(text[i])) return false;
        while (i < n && is_digit(text[i])) ++i;
    }
    if (i < n && (text[i] == 'e' || text[i] == 'E')) {
        ++i;
        if (i < n && (text[i] == '+' || text[i] == '-')) ++i;
        if (i >= n || !is_digit(text[i])) return false;
        while (i < n && is_digit(text[i])) ++i;
    }
    return i == n;
}

} // namespace lite_db::json
