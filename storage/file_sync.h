#pragma once
#include <string>



namespace lite_db {

[[nodiscard]] bool sync_file(const std::string& path) noexcept;

[[nodiscard]] bool sync_directory(const std::string& path) noexcept;

} // namespace lite_db
