#include "server/tcp_server.h"
#include "database/database.h"
#include <iostream>

int main() {
    Database db("data.db");
    std::cout << "[System] Loading configuration from setup.yaml..." << std::endl;
    db.load_config("setup.yaml");
    // Восстанавливаем буфер из лога (если сервер упал в прошлый раз)
    db.recover_from_wal();
    TcpServer server(5555, &db);
    server.start();
    return 0;
}