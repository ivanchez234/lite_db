#pragma once
#include <string>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
#else
    #include <netinet/in.h>
    #include <unistd.h>
#endif

#include "../database/database.h"

class TcpServer {
private:
    int port;
    int server_fd;
    Database* database;

public:
    TcpServer(int port, Database* db);
    void start();
    void handleClient(int client_socket);
};
