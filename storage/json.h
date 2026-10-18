#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>


namespace lite_db::json {

struct Value {

    std::string text;
    bool is_string = false;
};

using Field  = std::pair<std::string, Value>;
using Fields = std::vector<Field>;

bool parse_object(std::string_view input, Fields& out, std::string& error);

bool parse_object_prefix(std::string_view input, size_t& consumed, Fields& out, std::string& error);

std::string quote(std::string_view text);

bool is_number(std::string_view text) noexcept;

} // namespace lite_db::json
