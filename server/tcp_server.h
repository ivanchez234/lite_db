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
    // true — не выполнять, а сразу ответить text как ошибкой. Так ответ
    // об ошибке протокола встаёт в общую очередь и уходит клиенту по порядку.
    bool reject = false;
};

// Соединение с клиентом и его собственная очередь команд.
//
// Команды одного клиента обязаны выполняться по порядку: иначе ответы
// возвращаются вперемешку и клиент не может сопоставить их с запросами.
// Поэтому соединением в каждый момент занят ровно один рабочий поток,
// а параллелизм остаётся между разными соединениями.
//
// Время жизни: соединение живёт, пока на него ссылается поток чтения либо
// общая очередь. Сокет закрывается, когда исчезает последняя ссылка, — то есть
// никогда раньше, чем рабочий поток отправит последний ответ.
struct Connection {
    explicit Connection(SOCKET sock) noexcept : socket(sock) {}

    net::SocketHandle socket;

    // false — запись в сокет не удалась, клиенту больше ничего не отправить.
    std::atomic<bool> alive{true};
    // Поток чтения завершился, его можно присоединить (join).
    std::atomic<bool> readerDone{false};

    std::mutex              mutex;      // защищает pending и scheduled
    std::condition_variable space;      // в pending освободилось место
    std::deque<Request>     pending;    // команды в порядке поступления
    bool                    scheduled = false;  // уже стоит в общей очереди
};

// Многопоточный TCP-сервер.
//
//   поток run()        — принимает соединения, на каждое заводит поток чтения
//   потоки чтения      — режут поток байтов на строки-команды
//   рабочие потоки     — выполняют команды в базе и отправляют ответы
//
// Ни один поток не отсоединяется (detach): run() перед возвратом дожидается
// всех, поэтому после него Database можно безопасно разрушать.
class TcpServer {
public:
    TcpServer(int port, Database* db, unsigned workerCount = 5);

    // Вызывать только после того, как run() вернул управление
    // (или если run() вообще не запускался).
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Занимает порт и запускает рабочие потоки. false — порт занять не удалось.
    [[nodiscard]] bool open();

    // Принимает соединения, пока не вызван stop() или не нажат Ctrl+C.
    // Возвращается, когда все потоки сервера завершены.
    void run();

    // Просит run() завершиться. Можно вызывать из любого потока,
    // но не из обработчика сигнала — для него есть lite_db::request_shutdown().
    void stop();

    // Реальный порт. Если в конструктор передан 0, система выбирает
    // свободный порт сама — так тесты не конфликтуют друг с другом.
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
        std::thread                 reader;
    };

    int               port_;
    Database*         database_;
    unsigned          workerCount_;
    net::SocketHandle listener_;

    // Очередь соединений, у которых есть невыполненные команды.
    // Каждое соединение стоит в ней не больше одного раза (флаг scheduled),
    // поэтому её длина ограничена числом соединений.
    std::queue<std::shared_ptr<Connection>> readyQueue_;
    std::mutex                              queueMutex_;  // защищает readyQueue_ и запись stopFlag_
    std::condition_variable                 queueCV_;     // будит рабочие потоки

    std::vector<std::thread> workers_;

    // Потоки чтения. Трогает только поток run(), поэтому без мьютекса.
    std::list<Client> clients_;

    // Меняется только под queueMutex_: иначе рабочий поток может проверить
    // условие, не увидеть флаг, и уснуть уже после notify_all() — навсегда.
    std::atomic<bool> stopFlag_{false};

    // Как часто заблокированные потоки просыпаются проверить, не пора ли стоп.
    static constexpr int         kPollIntervalMs    = 200;
    // Поток на соединение, поэтому их число надо ограничивать.
    static constexpr std::size_t kMaxConnections    = 256;
    // Сколько команд одного клиента готовы держать в памяти. Дальше перестаём
    // читать из сокета, и TCP сам притормаживает клиента.
    static constexpr std::size_t kMaxPendingPerConn = 10000;
    // Клиент, шлющий байты без перевода строки, не должен занимать память без предела.
    static constexpr std::size_t kMaxLineBytes      = 1u << 20;
    // Сколько ждать, пока клиент, получивший ошибку протокола, закроет соединение.
    static constexpr std::chrono::seconds kDiscardTimeout{10};

#ifdef _WIN32
    bool netInitialised_ = false;
#endif
};
