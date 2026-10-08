#pragma once

// Остановка сервера по Ctrl+C.
//
// Обработчик сигнала только поднимает флаг: внутри него нельзя брать мьютексы,
// выделять память и писать в std::cout. Всю настоящую работу — сброс данных
// на диск, закрытие журнала, ожидание потоков — делает основной поток, когда
// замечает флаг.

namespace lite_db {

// Ctrl+C / SIGINT / SIGTERM на POSIX, Ctrl+C / Ctrl+Break / закрытие окна
// консоли на Windows. Заодно отключает SIGPIPE на POSIX.
void install_shutdown_handlers();

bool shutdown_requested() noexcept;

// Безопасно вызывать из обработчика сигнала.
void request_shutdown() noexcept;

} // namespace lite_db
