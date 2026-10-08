#pragma once

#include "socket_handle.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "../database/database.h"

// Соединение с клиентом и его собственная очередь команд.
//
// Команды одного клиента обязаны выполняться по порядку: иначе ответы
// возвращаются вперемешку и клиент не может сопоставить их с запросами.
// Поэтому соединением в каждый момент занят ровно один рабочий поток,
// а параллелизм остаётся между разными соединениями.
//
// Время жизни: соединение живёт, пока на него ссылается сетевой поток либо
// общая очередь. Иначе сокет закрылся бы при отключении клиента, а воркер
// успел бы отправить ответ в освобождённый дескриптор — который система
// к тому моменту могла выдать уже другому клиенту.
struct Connection {
    explicit Connection(SOCKET sock) noexcept : socket(sock) {}

    net::SocketHandle socket;
    std::atomic<bool> alive{true};

    std::mutex              mutex;    // защищает pending и scheduled
    std::deque<std::string> pending;  // команды в порядке поступления
    bool                    scheduled = false;  // уже стоит в общей очереди
};

class TcpServer {
public:
    TcpServer(int port, Database* db);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();

private:
    bool openListeningSocket();
    void handleClient(std::shared_ptr<Connection> connection);
    void workerThread(int workerId);
    void enqueueCommand(const std::shared_ptr<Connection>& connection, std::string command);

    int               port;
    net::SocketHandle listener;
    Database*         database;

    // Очередь соединений, у которых есть невыполненные команды.
    std::queue<std::shared_ptr<Connection>> readyQueue;
    std::mutex                              queueMutex;
    std::condition_variable                 queueCV;     // будит воркеров
    std::condition_variable                 producerCV;  // тормозит сеть при перегрузке

    std::vector<std::thread> workers;
    std::atomic<bool>        stopFlag{false};
    std::atomic<int>         activeConnections{0};

    // Поток на соединение, поэтому их число надо ограничивать.
    static constexpr int         kMaxConnections    = 256;
    // Сколько команд одного клиента готовы держать в памяти.
    static constexpr std::size_t kMaxPendingPerConn = 10000;
    // Клиент, шлющий байты без разделителя строк, не должен занимать память без предела.
    static constexpr std::size_t kMaxPendingBytes   = 1u << 20;

#ifdef _WIN32
    bool netInitialised = false;
#endif
};
