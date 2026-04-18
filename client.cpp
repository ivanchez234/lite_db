#include <iostream>
#include <string>
#include <winsock2.h>
#include <ws2tcpip.h>

// Сообщаем компилятору, что нам нужна библиотека ws2_32.lib
#pragma comment(lib, "ws2_32.lib")

int main() {
    // 1. Инициализация Winsock (специфично для Windows)
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed!" << std::endl;
        return 1;
    }

    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(5555);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Connection failed! Error: " << WSAGetLastError() << std::endl;
        WSACleanup();
        return 1;
    }

    std::cout << "Connected to LiteDB (Windows Mode)" << std::endl;
    
    while (true) {
        std::string cmd;
        std::cout << "> ";
        std::getline(std::cin, cmd);
        if (cmd == "exit") break;

        send(sock, cmd.c_str(), (int)cmd.size(), 0);
        
        char buf[1024] = {0};
        int bytes = recv(sock, buf, 1024, 0);
        if (bytes <= 0) {
            std::cout << "Server disconnected." << std::endl;
            break;
        }
        std::cout << "Server: " << buf << std::endl;
    }

    closesocket(sock);
    WSACleanup();
    return 0;
}