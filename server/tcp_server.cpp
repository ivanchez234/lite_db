#include "tcp_server.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <utility>

#ifdef _WIN32
    #pragma comment(lib, "ws2_32.lib")
#endif

namespace {

// Убирает пробелы и точки с запятой по краям команды.
std::string trim_command(const std::string& text) {
    const size_t first = text.find_first_not_of(" \n\r\t;");
    if (first == std::string::npos) return "";
    const size_t last = text.find_last_not_of(" \n\r\t;");
    return text.substr(first, last - first + 1);
}

void report_socket_error(const char* what) {
#ifdef _WIN32
    std::cerr << "[Network] " << what << " failed, WSA code "
              << WSAGetLastError() << std::endl;
#else
    std::cerr << "[Network] " << what << " failed: "
              << std::strerror(errno) << std::endl;
#endif
}

} // namespace

TcpServer::TcpServer(int p, Database* db) : port(p), database(db) {}

TcpServer::~TcpServer() {
    stopFlag.store(true);
    queueCV.notify_all();
    producerCV.notify_all();

    for (std::thread& worker : workers) {
        if (worker.joinable()) worker.join();
    }

    listener.reset();

#ifdef _WIN32
    if (netInitialised) WSACleanup();
#endif
}

bool TcpServer::openListeningSocket() {
    const SOCKET fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd == INVALID_SOCKET) {
        report_socket_error("socket");
        return false;
    }

    // Владение сразу: любой выход ниже закроет сокет сам.
    net::SocketHandle candidate(fd);

    // Без этого перезапуск сервера упирается в порт, висящий в TIME_WAIT.
    int reuse = 1;
    ::setsockopt(candidate.get(), SOL_SOCKET, SO_REUSEADDR,
                 reinterpret_cast<const char*>(&reuse), sizeof(reuse));

    sockaddr_in address{};
    address.sin_family      = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port        = htons(static_cast<unsigned short>(port));

    if (::bind(candidate.get(), reinterpret_cast<sockaddr*>(&address),
               sizeof(address)) == SOCKET_ERROR) {
        report_socket_error("bind");
        return false;
    }

    if (::listen(candidate.get(), SOMAXCONN) == SOCKET_ERROR) {
        report_socket_error("listen");
        return false;
    }

    listener = std::move(candidate);
    return true;
}

void TcpServer::workerThread(int workerId) {
    for (;;) {
        std::shared_ptr<Connection> connection;

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCV.wait(lock, [this] {
                return !readyQueue.empty() || stopFlag.load();
            });

            if (readyQueue.empty()) return;  // проснулись на остановке

            connection = std::move(readyQueue.front());
            readyQueue.pop();
            producerCV.notify_one();
        }

        // Пока соединение у этого воркера, никто другой его команды не берёт,
        // поэтому ответы уходят клиенту в том же порядке, в каком пришли запросы.
        for (;;) {
            std::string command;

            {
                std::lock_guard<std::mutex> lock(connection->mutex);
                if (connection->pending.empty()) {
                    connection->scheduled = false;  // работы больше нет
                    break;
                }
                command = std::move(connection->pending.front());
                connection->pending.pop_front();
            }

            std::string response;

            try {
                response = database->execute(command);
            } catch (const std::exception& e) {
                std::cerr << "\n[Worker " << workerId << "] error: " << e.what()
                          << "\ncommand: " << command << std::endl;
                response = "ERR_INTERNAL_EXCEPTION";
            } catch (...) {
                std::cerr << "\n[Worker " << workerId << "] unknown error"
                          << "\ncommand: " << command << std::endl;
                response = "ERR_UNKNOWN_CRASH";
            }

            response += "\n";

            if (!connection->alive.load()) {
                // Клиент отключился: дочитываем очередь, но не пишем в сокет.
                std::lock_guard<std::mutex> lock(connection->mutex);
                connection->pending.clear();
                connection->scheduled = false;
                break;
            }

            if (!net::send_all(connection->socket.get(), response)) {
                connection->alive.store(false);
            }
        }
    }
}

// Кладёт команду в очередь соединения и, если оно ещё не ждёт обработки,
// ставит его в общую очередь.
void TcpServer::enqueueCommand(const std::shared_ptr<Connection>& connection,
                               std::string command) {
    bool needs_scheduling = false;

    {
        std::lock_guard<std::mutex> lock(connection->mutex);
        if (connection->pending.size() >= kMaxPendingPerConn) return;  // клиент завалил нас командами
        connection->pending.push_back(std::move(command));

        if (!connection->scheduled) {
            connection->scheduled = true;
            needs_scheduling = true;
        }
    }

    if (!needs_scheduling) return;

    {
        std::unique_lock<std::mutex> lock(queueMutex);
        producerCV.wait(lock, [this] {
            return readyQueue.size() < static_cast<std::size_t>(kMaxConnections) || stopFlag.load();
        });

        if (stopFlag.load()) return;
        readyQueue.push(connection);
    }

    queueCV.notify_one();
}

void TcpServer::start() {
#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "[Network] WSAStartup failed" << std::endl;
        return;
    }
    netInitialised = true;
#endif

    if (!openListeningSocket()) {
        std::cerr << "[Network] Server could not start on port " << port << std::endl;
        return;
    }

    const unsigned int worker_count = 5;
    std::cout << "[ThreadPool] Starting " << worker_count << " worker threads..." << std::endl;

    for (unsigned int i = 0; i < worker_count; ++i) {
        workers.emplace_back(&TcpServer::workerThread, this, static_cast<int>(i));
    }

    std::cout << "Server started on port " << port << std::endl;

    while (!stopFlag.load()) {
        const SOCKET client = ::accept(listener.get(), nullptr, nullptr);

        if (client == INVALID_SOCKET) {
#ifndef _WIN32
            if (errno == EINTR) continue;  // прервано сигналом, не ошибка
#endif
            report_socket_error("accept");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        // Сокет сразу под владением: дальше он закроется сам в любом случае.
        auto connection = std::make_shared<Connection>(client);

        if (activeConnections.load() >= kMaxConnections) {
            // Отказываем явно, вместо того чтобы плодить потоки без предела.
            net::send_all(connection->socket.get(), "ERR_TOO_MANY_CONNECTIONS\n");
            continue;
        }

        activeConnections.fetch_add(1);

        std::thread([this, connection]() {
            try {
                handleClient(connection);
            } catch (const std::exception& e) {
                std::cerr << "\n[Network] connection error: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "\n[Network] unknown connection error" << std::endl;
            }

            // Сокет закроется, когда задачи этого клиента покинут очередь
            // и умрёт последняя ссылка на Connection.
            connection->alive.store(false);
            activeConnections.fetch_sub(1);
        }).detach();
    }
}

void TcpServer::handleClient(std::shared_ptr<Connection> connection) {
    char        buffer[4096];
    std::string pending;

    for (;;) {
        const auto received = ::recv(connection->socket.get(), buffer, sizeof(buffer), 0);

        if (received == 0) break;  // клиент закрыл соединение

        if (received < 0) {
#ifndef _WIN32
            if (errno == EINTR) continue;
#endif
            break;
        }

        pending.append(buffer, static_cast<size_t>(received));

        if (pending.size() > kMaxPendingBytes) {
            net::send_all(connection->socket.get(), "ERR_COMMAND_TOO_LONG\n");
            break;
        }

        // TCP не сохраняет границы сообщений, поэтому команды собираются
        // из потока байтов по разделителю.
        size_t newline;
        while ((newline = pending.find('\n')) != std::string::npos) {
            std::string command = trim_command(pending.substr(0, newline));
            pending.erase(0, newline + 1);

            if (command.empty()) continue;

            enqueueCommand(connection, std::move(command));
            if (stopFlag.load()) return;
        }
    }
}
