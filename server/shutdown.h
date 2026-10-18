#pragma once
// Остановка сервера по Ctrl+C.

namespace lite_db {


void install_shutdown_handlers();

bool shutdown_requested() noexcept;

void request_shutdown() noexcept;

} // namespace lite_db
