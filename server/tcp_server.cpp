#include "tcp_server.h"
#include <iostream>
#include <thread>

#ifdef _WIN32
    #pragma comment(lib, "ws2_32.lib")
#endif

TcpServer::TcpServer(int p, Database* db) {
    port = p;
    database = db;
}

void TcpServer::start() {
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    bind(server_fd, (sockaddr*)&address, sizeof(address));
    listen(server_fd, 5);

    std::cout << "Server started on port " << port << std::endl;

    while (true) {
        int client_socket = accept(server_fd, nullptr, nullptr);

        std::thread([this, client_socket]() {
            handleClient(client_socket);
        }).detach();
    }
}

void TcpServer::handleClient(int client_socket) {
    char buffer[1024];

    while (true) {
#ifdef _WIN32
        int bytes = recv(client_socket, buffer, sizeof(buffer), 0);
#else
        int bytes = read(client_socket, buffer, sizeof(buffer));
#endif

        if (bytes <= 0) break;

        std::string request(buffer, bytes);
        std::string response = database->execute(request);

#ifdef _WIN32
        send(client_socket, response.c_str(), response.size(), 0);
#else
        write(client_socket, response.c_str(), response.size());
#endif
    }

#ifdef _WIN32
    closesocket(client_socket);
#else
    close(client_socket);
#endif
}
