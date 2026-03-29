#include "server/tcp_server.h"
#include "database/database.h"

int main() {
    Database db("data.db");
    TcpServer server(5555, &db);
    server.start();
    return 0;
}