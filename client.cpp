// Консольный клиент lite_db: читает команды из stdin, печатает ответы сервера.
//
// Протокол построчный: одна команда — одна строка, один ответ — одна строка.
//   client_app [host] [port]      по умолчанию 127.0.0.1 5555

#include "server/socket_handle.h"

#include <iostream>
#include <string>

namespace {

// Читает из сокета одну строку ответа. Байты, пришедшие после '\n',
// остаются в buffered и пойдут в следующий ответ.
bool read_line(SOCKET sock, std::string& buffered, std::string& line) {
    for (;;) {
        const size_t newline = buffered.find('\n');
        if (newline != std::string::npos) {
            line = buffered.substr(0, newline);
            buffered.erase(0, newline + 1);
            return true;
        }

        char chunk[4096];
        const auto received = ::recv(sock, chunk, static_cast<int>(sizeof(chunk)), 0);
        if (received > 0) {
            buffered.append(chunk, static_cast<size_t>(received));
            continue;
        }
        if (received < 0 && net::interrupted()) continue;
        return false;  // сервер закрыл соединение или ошибка
    }
}

} // namespace

int main(int argc, char** argv) {
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const int         port = argc > 2 ? std::stoi(argv[2]) : 5555;

#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed" << std::endl;
        return 1;
    }
#endif

    int exit_code = 0;
    {
        net::SocketHandle sock(::socket(AF_INET, SOCK_STREAM, 0));
        if (!sock.valid()) {
            std::cerr << "Could not create socket" << std::endl;
            exit_code = 1;
        } else {
            // Без задержки Нейгла: короткие команды уходят сразу.
            net::set_no_delay(sock.get());

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port   = htons(static_cast<unsigned short>(port));

            if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
                std::cerr << "Bad address: " << host << std::endl;
                exit_code = 1;
            } else if (::connect(sock.get(), reinterpret_cast<sockaddr*>(&addr),
                                 sizeof(addr)) == SOCKET_ERROR) {
                std::cerr << "Connection to " << host << ":" << port << " failed" << std::endl;
                exit_code = 1;
            } else {
                std::cout << "Connected to LiteDB at " << host << ":" << port
                          << " (type 'exit' to quit)" << std::endl;

                std::string buffered;
                std::string command;
                std::string response;

                while (std::cout << "> " && std::getline(std::cin, command)) {
                    if (command == "exit") break;
                    if (command.empty()) continue;

                    if (!net::send_all(sock.get(), command + "\n")) {
                        std::cout << "Server disconnected." << std::endl;
                        break;
                    }
                    if (!read_line(sock.get(), buffered, response)) {
                        std::cout << "Server disconnected." << std::endl;
                        break;
                    }
                    std::cout << response << std::endl;
                }
            }
        }
    }   // сокет закрывается до WSACleanup

#ifdef _WIN32
    WSACleanup();
#endif
    return exit_code;
}
