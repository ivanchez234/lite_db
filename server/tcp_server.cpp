#include "tcp_server.h"
#include <iostream>
#include <thread>
#include <sstream> // Исправляет "incomplete type std::stringstream"
#include <string>  // Исправляет "no instance of std::getline"

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

void TcpServer::handleClient(SOCKET clientSocket) {
    char buffer[4096];
    
    while (true) {
        // Читаем данные из сокета
        int bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (bytesRead <= 0) break;

        buffer[bytesRead] = '\0'; // Гарантируем конец строки
        std::string fullData(buffer);
        std::stringstream ss(fullData);
        std::string singleCommand;

        // Разделяем входную строку по символу ';'
        while (std::getline(ss, singleCommand, ';')) {
            
            // 1. Убираем мусор (пробелы, табы, переносы) по краям команды
            size_t first = singleCommand.find_first_not_of(" \n\r\t");
            if (first == std::string::npos) continue; // Строка пустая или из одних пробелов
            size_t last = singleCommand.find_last_not_of(" \n\r\t");
            singleCommand = singleCommand.substr(first, (last - first + 1));

            if (singleCommand.empty()) continue;

            // 2. Выполняем команду (используй то имя переменной, которое у тебя в классе)
            std::string result = database->execute(singleCommand);
            
            // 3. Отправляем ответ клиенту
            send(clientSocket, result.c_str(), (int)result.size(), 0);
        }
    }

    // Закрываем сокет
#ifdef _WIN32
    closesocket(clientSocket);
#else
    close(clientSocket);
#endif
}
