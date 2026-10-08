#include "tcp_server.h"
#include "shutdown.h"

#include <chrono>
#include <iostream>
#include <system_error>
#include <utility>

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
    // system_category().message вместо strerror: strerror не потокобезопасен.
    std::cerr << "[Network] " << what << " failed: "
              << std::system_category().message(errno) << std::endl;
#endif
}

} // namespace

TcpServer::TcpServer(int port, Database* db, unsigned workerCount, std::string host)
    : port_(port), host_(std::move(host)), database_(db),
      workerCount_(workerCount == 0 ? 1 : workerCount) {}

TcpServer::~TcpServer() {
    stop();
    shutdownAll();
}

// --- ЗАПУСК -----------------------------------------------------------------

bool TcpServer::open() {
#ifdef _WIN32
    if (!netInitialised_) {
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
            std::cerr << "[Network] WSAStartup failed" << std::endl;
            return false;
        }
        netInitialised_ = true;
    }
#endif

    if (!openListeningSocket()) {
        std::cerr << "[Network] Server could not start on port " << port_ << std::endl;
        return false;
    }

    std::cout << "[ThreadPool] Starting " << workerCount_ << " worker threads..." << std::endl;
    for (unsigned i = 0; i < workerCount_; ++i) {
        workers_.emplace_back(&TcpServer::workerThread, this, static_cast<int>(i));
    }

    std::cout << "Server started on " << host_ << ":" << port_ << std::endl;
    return true;
}

bool TcpServer::openListeningSocket() {
    const SOCKET fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd == INVALID_SOCKET) {
        report_socket_error("socket");
        return false;
    }

    // Владение сразу: любой выход ниже закроет сокет сам.
    net::SocketHandle candidate(fd);

#ifndef _WIN32
    // Без этого перезапуск сервера упирается в порт, висящий в TIME_WAIT.
    // На Windows SO_REUSEADDR значит другое — позволяет чужому процессу
    // занять тот же порт, — поэтому там его не ставим.
    int reuse = 1;
    ::setsockopt(candidate.get(), SOL_SOCKET, SO_REUSEADDR,
                 reinterpret_cast<const char*>(&reuse), sizeof(reuse));
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port   = htons(static_cast<unsigned short>(port_));
    if (inet_pton(AF_INET, host_.c_str(), &address.sin_addr) != 1) {
        std::cerr << "[Network] Bad listen address: " << host_ << std::endl;
        return false;
    }

    if (::bind(candidate.get(), reinterpret_cast<sockaddr*>(&address),
               sizeof(address)) == SOCKET_ERROR) {
        report_socket_error("bind");
        return false;
    }

    if (::listen(candidate.get(), SOMAXCONN) == SOCKET_ERROR) {
        report_socket_error("listen");
        return false;
    }

    // Узнаём, какой порт достался на самом деле (важно, если просили 0).
    sockaddr_in bound{};
#ifdef _WIN32
    int boundLen = sizeof(bound);
#else
    socklen_t boundLen = sizeof(bound);
#endif
    if (::getsockname(candidate.get(), reinterpret_cast<sockaddr*>(&bound), &boundLen) == 0) {
        port_ = ntohs(bound.sin_port);
    }

    listener_ = std::move(candidate);
    return true;
}

// --- ОСТАНОВКА ----------------------------------------------------------------

bool TcpServer::stopping() const noexcept {
    return stopFlag_.load() || lite_db::shutdown_requested();
}

void TcpServer::stop() {
    {
        // Флаг меняется под тем же мьютексом, под которым рабочие потоки
        // проверяют условие ожидания. Иначе возможна потерянная побудка:
        // поток проверил флаг (ещё false), мы поставили true и вызвали
        // notify_all(), а поток только после этого уснул — и спит вечно.
        std::lock_guard<std::mutex> lock(queueMutex_);
        stopFlag_.store(true);
    }
    queueCV_.notify_all();
}

void TcpServer::shutdownAll() {
    // 1. Потоки чтения сами замечают остановку не позже чем через
    //    kPollIntervalMs и выходят. Ждём их первыми: пока они живы, они
    //    могут добавить работу в очередь.
    for (Client& client : clients_) {
        if (client.reader.joinable()) client.reader.join();
    }
    clients_.clear();

    // 2. Рабочие потоки разбирают остаток очереди (не выполняя команды)
    //    и выходят, когда она пуста.
    for (std::thread& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
    workers_.clear();

    listener_.reset();

#ifdef _WIN32
    if (netInitialised_) {
        WSACleanup();
        netInitialised_ = false;
    }
#endif
}

// --- ПРИЁМ СОЕДИНЕНИЙ ---------------------------------------------------------

void TcpServer::run() {
    if (!listener_.valid()) {
        std::cerr << "[Network] run() called before successful open()" << std::endl;
        return;
    }

    acceptLoop();

    std::cout << "[Network] Stopping: waiting for connections and workers..." << std::endl;
    stop();          // если остановка пришла сигналом, разбудить рабочие потоки
    shutdownAll();
    std::cout << "[Network] Server stopped" << std::endl;
}

void TcpServer::acceptLoop() {
    while (!stopping()) {
        reapFinishedClients();

        // Не засыпаем в accept() навсегда: раз в kPollIntervalMs
        // проверяем, не пора ли остановиться.
        const int ready = net::wait_readable(listener_.get(), kPollIntervalMs);
        if (ready == 0) continue;
        if (ready < 0) {
            if (net::interrupted()) continue;  // прервано сигналом (например, Ctrl+C)
            report_socket_error("wait for connection");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        const SOCKET client = ::accept(listener_.get(), nullptr, nullptr);
        if (client == INVALID_SOCKET) {
            if (net::interrupted()) continue;
            report_socket_error("accept");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        acceptClient(client);
    }
}

void TcpServer::acceptClient(SOCKET sock) {
    // Сокет сразу под владением: дальше он закроется сам в любом случае.
    auto connection = std::make_shared<Connection>(sock);
#ifndef LITE_DB_EXPERIMENT_NAGLE
    net::set_no_delay(sock);
#endif

    if (clients_.size() >= kMaxConnections) {
        // Отказываем явно, вместо того чтобы плодить потоки без предела.
        (void)net::send_all(connection->socket.get(), "ERR_TOO_MANY_CONNECTIONS\n");
        return;
    }

    try {
        Client client;
        client.connection = connection;
        client.reader = std::thread(&TcpServer::readLoop, this, connection);
        clients_.push_back(std::move(client));
    } catch (const std::system_error& e) {
        // Система не дала создать поток — отказываем этому клиенту, сервер живёт.
        std::cerr << "[Network] Could not start connection thread: " << e.what() << std::endl;
        connection->readerDone.store(true);
    }
}

void TcpServer::reapFinishedClients() {
    for (auto it = clients_.begin(); it != clients_.end();) {
        if (it->connection->readerDone.load()) {
            if (it->reader.joinable()) it->reader.join();
            it = clients_.erase(it);
        } else {
            ++it;
        }
    }
}

// --- ЧТЕНИЕ КОМАНД ------------------------------------------------------------

void TcpServer::readLoop(const std::shared_ptr<Connection>& connection) {
    // Исключение, вылетевшее из функции потока, вызывает std::terminate —
    // весь сервер упал бы из-за одного клиента.
    try {
        readCommands(connection);
    } catch (const std::exception& e) {
        std::cerr << "\n[Network] connection error: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "\n[Network] unknown connection error" << std::endl;
    }

    // Сокет здесь не закрываем: в очереди могут оставаться команды этого
    // клиента. Он закроется, когда исчезнет последняя ссылка на Connection.
    connection->readerDone.store(true);
}

void TcpServer::readCommands(const std::shared_ptr<Connection>& connection) {
    const SOCKET sock = connection->socket.get();
    char         buffer[4096];
    std::string  partial;   // начало строки, конец которой ещё не пришёл

    while (!stopping() && connection->alive.load()) {
        const int ready = net::wait_readable(sock, kPollIntervalMs);
        if (ready == 0) continue;
        if (ready < 0) {
            if (net::interrupted()) continue;
            return;
        }

        const auto received = ::recv(sock, buffer, static_cast<int>(sizeof(buffer)), 0);

        if (received == 0) {
            // Клиент закончил ОТПРАВКУ — но, возможно, ещё ждёт ответов
            // (так делает, например, `nc` после конца ввода). Поэтому
            // соединение не рвём: рабочий поток доотправит всё, что есть.
            return;
        }
        if (received < 0) {
            if (net::interrupted()) continue;
            return;
        }

        partial.append(buffer, static_cast<size_t>(received));

        // TCP не сохраняет границы сообщений, поэтому команды собираются
        // из потока байтов по разделителю.
        size_t newline;
        while ((newline = partial.find('\n')) != std::string::npos) {
            std::string command = trim_command(partial.substr(0, newline));
            partial.erase(0, newline + 1);

            if (command.empty()) continue;
            if (!enqueueRequest(connection, Request{std::move(command), false})) return;
        }

        if (partial.size() > kMaxLineBytes) {
            // Ответ об ошибке идёт через общую очередь, чтобы не обогнать
            // ответы на команды, присланные раньше. Команды этого клиента
            // больше не выполняем.
            (void)enqueueRequest(connection, Request{"ERR_COMMAND_TOO_LONG", true});
            discardInput(connection);
            return;
        }
    }
}

// Дочитывает и выбрасывает всё, что клиент ещё шлёт, пока он не закроет
// соединение (но не дольше kDiscardTimeout).
//
// Зачем не закрыть сокет сразу: если в нём остались непрочитанные данные,
// система отвечает клиенту не FIN, а RST. А RST на Windows (и иногда на Linux)
// уничтожает у клиента уже пришедшие, но ещё не прочитанные данные — то есть
// клиент может так и не увидеть наш ответ с ошибкой.
void TcpServer::discardInput(const std::shared_ptr<Connection>& connection) {
    const SOCKET sock = connection->socket.get();
    const auto deadline = std::chrono::steady_clock::now() + kDiscardTimeout;
    char sink[4096];

    while (!stopping() && connection->alive.load()
           && std::chrono::steady_clock::now() < deadline) {
        const int ready = net::wait_readable(sock, kPollIntervalMs);
        if (ready == 0) continue;
        if (ready < 0 && net::interrupted()) continue;
        if (ready < 0) return;

        const auto received = ::recv(sock, sink, static_cast<int>(sizeof(sink)), 0);
        if (received == 0) return;                       // клиент закрыл соединение
        if (received < 0 && !net::interrupted()) return;
    }
}

// Кладёт команду в очередь соединения и, если оно ещё не ждёт обработки,
// ставит его в общую очередь. false — сервер останавливается или клиент пропал.
bool TcpServer::enqueueRequest(const std::shared_ptr<Connection>& connection, Request request) {
    bool needsScheduling = false;

    {
        std::unique_lock<std::mutex> lock(connection->mutex);

        // Клиент шлёт быстрее, чем мы выполняем. Не выбрасываем команды
        // (клиент ждал бы ответа вечно), а перестаём читать сокет: буфер
        // приёма заполнится, и TCP притормозит отправителя сам.
        while (connection->pending.size() >= kMaxPendingPerConn) {
            if (stopping() || !connection->alive.load()) return false;
            // С таймаутом: остановку сервера нужно заметить, даже если
            // рабочий поток так и не освободит место.
            connection->space.wait_for(lock, std::chrono::milliseconds(kPollIntervalMs));
        }

        connection->pending.push_back(std::move(request));

        if (!connection->scheduled) {
            connection->scheduled = true;
            needsScheduling = true;
        }
    }

    if (needsScheduling) {
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            readyQueue_.push(connection);
        }
        queueCV_.notify_one();
    }
    return true;
}

// --- ВЫПОЛНЕНИЕ КОМАНД ------------------------------------------------------------

void TcpServer::workerThread(int workerId) {
    for (;;) {
        std::shared_ptr<Connection> connection;

        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            queueCV_.wait(lock, [this] {
                return !readyQueue_.empty() || stopFlag_.load();
            });

            // Остановка и очередь пуста — работы больше не будет.
            if (readyQueue_.empty()) return;

            connection = std::move(readyQueue_.front());
            readyQueue_.pop();
        }

        serveConnection(*connection, workerId);
    }
}

void TcpServer::serveConnection(Connection& connection, int workerId) {
    // Пока соединение у этого потока, никто другой его команды не берёт,
    // поэтому ответы уходят клиенту в том же порядке, в каком пришли запросы.
    for (;;) {
        Request request;

        {
            std::lock_guard<std::mutex> lock(connection.mutex);

            // При остановке и после потери клиента оставшиеся команды не
            // выполняем: подтверждения по ним клиент не получал, значит,
            // и выполненными их считать не должен.
            if (stopFlag_.load() || !connection.alive.load()) connection.pending.clear();

            if (connection.pending.empty()) {
                connection.scheduled = false;  // работы больше нет
                break;
            }

            request = std::move(connection.pending.front());
            connection.pending.pop_front();
        }
        connection.space.notify_one();  // поток чтения мог ждать места в очереди

        std::string response = request.reject ? request.text
                                              : executeSafely(request.text, workerId);
        response += '\n';

        if (!net::send_all(connection.socket.get(), response)) {
            connection.alive.store(false);
        }
    }
}

std::string TcpServer::executeSafely(const std::string& command, int workerId) {
    try {
        return database_->execute(command);
    } catch (const std::exception& e) {
        std::cerr << "\n[Worker " << workerId << "] error: " << e.what()
                  << "\ncommand: " << command << std::endl;
        return "ERR_INTERNAL_EXCEPTION";
    } catch (...) {
        std::cerr << "\n[Worker " << workerId << "] unknown error"
                  << "\ncommand: " << command << std::endl;
        return "ERR_UNKNOWN_CRASH";
    }
}
