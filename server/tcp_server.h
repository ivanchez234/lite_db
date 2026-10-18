#pragma once

#include "socket_handle.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "../database/database.h"

// Одна строка от клиента, ожидающая обработки.
struct Request {
    std::string text;

    bool reject = false;
};


struct Connection {
    explicit Connection(SOCKET sock) noexcept : socket(sock) {}

    net::SocketHandle socket;

    // false — запись в сокет не удалась, клиенту больше ничего не отправить.
    std::atomic<bool> alive{true};
    // Поток чтения завершился, его можно присоединить (join).
    std::atomic<bool> readerDone{false};

    std::mutex mutex;      
    std::condition_variable space;      
    std::deque<Request> pending;    
    bool scheduled = false;  
};


class TcpServer {
public:

    TcpServer(int port, Database* db, unsigned workerCount = 5,
              std::string host = "127.0.0.1");

    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Занимает порт и запускает рабочие потоки
    [[nodiscard]] bool open();

    // Принимает соединения
    void run();

    void stop();

    // на случай если настоящмй порт
    int port() const noexcept { return port_; }

private:
    bool openListeningSocket();
    bool stopping() const noexcept;

    void acceptLoop();
    void acceptClient(SOCKET sock);
    void reapFinishedClients();

    void readLoop(const std::shared_ptr<Connection>& connection);
    void readCommands(const std::shared_ptr<Connection>& connection);
    void discardInput(const std::shared_ptr<Connection>& connection);
    bool enqueueRequest(const std::shared_ptr<Connection>& connection, Request request);

    void workerThread(int workerId);
    void serveConnection(Connection& connection, int workerId);
    std::string executeSafely(const std::string& command, int workerId);

    void shutdownAll();

    struct Client {
        std::shared_ptr<Connection> connection;
        std::thread reader;
    };

    int port_;
    std::string host_;
    Database* database_;
    unsigned workerCount_;
    net::SocketHandle listener_;

    std::queue<std::shared_ptr<Connection>> readyQueue_;
    std::mutex queueMutex_;  
    std::condition_variable queueCV_;     

    std::vector<std::thread> workers_;

    // Потоки чтения
    std::list<Client> clients_;

    std::atomic<bool> stopFlag_{false};

    static constexpr int kPollIntervalMs = 200;
    static constexpr std::size_t kMaxConnections = 256;
    static constexpr std::size_t kMaxPendingPerConn = 10000;
    static constexpr std::size_t kMaxLineBytes = 1u << 20;
    static constexpr std::chrono::seconds kDiscardTimeout{10};

#ifdef _WIN32
    bool netInitialised_ = false;
#endif
};
