#include "database/database.h"
#include "server/shutdown.h"
#include "server/tcp_server.h"

#include <exception>
#include <iostream>
#include <string>

// server_app [port] [host]     по умолчанию 5555 и 127.0.0.1
//
// По умолчанию сервер доступен только с этой же машины: аутентификации нет.
// Чтобы открыть его в сеть, адрес нужно указать явно: server_app 5555 0.0.0.0
int main(int argc, char** argv) {
    // До создания потоков: обработчики сигналов ставятся на весь процесс.
    lite_db::install_shutdown_handlers();

    try {
        const int         port = argc > 1 ? std::stoi(argv[1]) : 5555;
        const std::string host = argc > 2 ? argv[2] : "127.0.0.1";

        // База объявлена раньше сервера, значит, разрушится позже него:
        // к моменту ~Database ни один поток сервера к ней уже не обращается.
        Database db("wal.log");
        std::cout << "[System] Loading configuration from setup.yaml..." << std::endl;
        db.load_config("setup.yaml");
        db.recover_from_wal();

        TcpServer server(port, &db, 5, host);
        if (!server.open()) return 1;

        std::cout << "[System] Press Ctrl+C to stop the server" << std::endl;
        server.run();   // вернётся после Ctrl+C, когда все потоки завершены

        // Дальше деструкторы: сервер (уже остановлен), затем база —
        // сброс буферов на диск, fsync и очистка журнала.
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[System] Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
