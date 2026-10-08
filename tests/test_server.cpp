// Проверка TCP-сервера через настоящие сокеты.
//
// Сервер поднимается в этом же процессе на свободном порту (порт 0 —
// система выбирает сама), клиенты — обычные блокирующие сокеты.
// Под ThreadSanitizer эти тесты ловят гонки между потоками чтения,
// рабочими потоками и остановкой сервера.

#include <catch2/catch_test_macros.hpp>

#include "database/database.h"
#include "server/socket_handle.h"
#include "server/tcp_server.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

const std::string kWal = "test_server_wal.log";

// Сервер вместе с базой и потоком, в котором крутится run().
class RunningServer {
public:
    RunningServer() {
        std::error_code ec;
        std::filesystem::remove_all("data", ec);
        std::filesystem::remove(kWal, ec);

        db_ = std::make_unique<Database>(kWal);
        REQUIRE(db_->execute("CREATE use").rfind("OK", 0) == 0);
        REQUIRE(db_->execute("SCHEMA use name:STRING age:INT") == "OK: Schema applied");

        server_ = std::make_unique<TcpServer>(0, db_.get(), 4);
        REQUIRE(server_->open());
        runner_ = std::async(std::launch::async, [this] { server_->run(); });
    }

    ~RunningServer() {
        server_->stop();
        runner_.wait();
    }

    int port() const { return server_->port(); }
    TcpServer& server() { return *server_; }

    // Ждёт завершения run() не дольше timeout. false — сервер завис.
    bool waitStopped(std::chrono::seconds timeout) {
        return runner_.wait_for(timeout) == std::future_status::ready;
    }

private:
    std::unique_ptr<Database>  db_;
    std::unique_ptr<TcpServer> server_;
    std::future<void>          runner_;
};

// Простой построчный клиент.
//
// Макросы Catch2 (REQUIRE и т. п.) не потокобезопасны, поэтому у клиента
// есть методы без проверок — для использования из рабочих потоков теста.
class Client {
public:
    explicit Client(int port) : sock_(::socket(AF_INET, SOCK_STREAM, 0)) {
        REQUIRE(tryConnect(port));
    }

    // Для рабочих потоков: без REQUIRE внутри.
    struct NoChecks {};
    Client(int port, NoChecks) : sock_(::socket(AF_INET, SOCK_STREAM, 0)) {
        connected_ = tryConnect(port);
    }

    bool connected() const { return connected_; }

    bool send(const std::string& data) { return net::send_all(sock_.get(), data); }

    // Одна строка ответа. false — соединение закрыто или ответа нет за 10 секунд.
    bool readLine(std::string& line) {
        for (;;) {
            const size_t newline = buffered_.find('\n');
            if (newline != std::string::npos) {
                line = buffered_.substr(0, newline);
                buffered_.erase(0, newline + 1);
                return true;
            }
            if (net::wait_readable(sock_.get(), 10000) <= 0) return false;

            char chunk[4096];
            const auto received = ::recv(sock_.get(), chunk, static_cast<int>(sizeof(chunk)), 0);
            if (received <= 0) return false;
            buffered_.append(chunk, static_cast<size_t>(received));
        }
    }

    std::string request(const std::string& command) {
        std::string line;
        REQUIRE(tryRequest(command, line));
        return line;
    }

    bool tryRequest(const std::string& command, std::string& line) {
        return send(command + "\n") && readLine(line);
    }

    void finishSending() { net::shutdown_send(sock_.get()); }
    SOCKET raw() const { return sock_.get(); }

private:
    bool tryConnect(int port) {
        if (!sock_.valid()) return false;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(static_cast<unsigned short>(port));
        if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) return false;
        return ::connect(sock_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    }

    net::SocketHandle sock_;
    std::string       buffered_;
    bool              connected_ = true;
};

std::string row(int id) {
    return R"({"name":"U)" + std::to_string(id) + R"(","age":)" + std::to_string(id % 90) + "}";
}

} // namespace

TEST_CASE("сервер выполняет команды и отвечает построчно", "[server]") {
    RunningServer srv;
    Client client(srv.port());

    REQUIRE(client.request("INSERT use 1 " + row(1)) == "OK");
    REQUIRE(client.request("SELECT use 1") == row(1));
    REQUIRE(client.request("SELECT use 2") == "ERR_NOT_FOUND");
    REQUIRE(client.request("SELECT * FROM use WHERE id = 1") == row(1));

    // Ответ SELECT ALL — тоже одна строка, иначе клиент принял бы его
    // за несколько ответов.
    REQUIRE(client.request("INSERT use 2 " + row(2)) == "OK");
    const std::string all = client.request("SELECT use ALL");
    REQUIRE(all.front() == '[');
    REQUIRE(all.back() == ']');
}

TEST_CASE("ответы на пачку команд приходят в порядке запросов", "[server]") {
    RunningServer srv;
    Client client(srv.port());

    // Всё одной отправкой: команды конвейером лежат в очереди соединения.
    constexpr int kCount = 300;
    std::string batch;
    for (int id = 1; id <= kCount; ++id) {
        batch += "INSERT use " + std::to_string(id) + " " + row(id) + "\n";
        batch += "SELECT use " + std::to_string(id) + "\n";
    }
    REQUIRE(client.send(batch));

    for (int id = 1; id <= kCount; ++id) {
        std::string inserted, selected;
        REQUIRE(client.readLine(inserted));
        REQUIRE(client.readLine(selected));
        REQUIRE(inserted == "OK");
        REQUIRE(selected == row(id));
    }
}

TEST_CASE("несколько клиентов параллельно не мешают друг другу", "[server][concurrency]") {
    RunningServer srv;

    constexpr int kClients = 8;
    constexpr int kPerClient = 100;
    std::atomic<int> errors{0};

    std::vector<std::thread> threads;
    for (int c = 0; c < kClients; ++c) {
        threads.emplace_back([&, c] {
            Client client(srv.port(), Client::NoChecks{});
            if (!client.connected()) {
                errors.fetch_add(1);
                return;
            }
            std::string reply;
            for (int n = 0; n < kPerClient; ++n) {
                const int id = c * 1000 + n;
                if (!client.tryRequest("INSERT use " + std::to_string(id) + " " + row(id), reply)
                    || reply != "OK")
                    errors.fetch_add(1);
                if (!client.tryRequest("SELECT use " + std::to_string(id), reply)
                    || reply != row(id))
                    errors.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) t.join();

    REQUIRE(errors.load() == 0);
}

TEST_CASE("клиент закончил отправку — ответы всё равно приходят", "[server]") {
    // Так работает, например, `echo ... | nc`: после конца ввода клиент
    // закрывает свою половину соединения, но ответы ещё читает.
    RunningServer srv;
    Client client(srv.port());

    REQUIRE(client.send("INSERT use 7 " + row(7) + "\nSELECT use 7\n"));
    client.finishSending();

    std::string first, second, extra;
    REQUIRE(client.readLine(first));
    REQUIRE(client.readLine(second));
    REQUIRE(first == "OK");
    REQUIRE(second == row(7));

    // Больше ответов нет, и сервер закрывает соединение сам.
    REQUIRE_FALSE(client.readLine(extra));
}

TEST_CASE("stop() завершает сервер, даже если клиенты остаются подключены", "[server][shutdown]") {
    RunningServer srv;
    Client idle(srv.port());   // подключён и молчит
    Client busy(srv.port());
    REQUIRE(busy.request("INSERT use 1 " + row(1)) == "OK");

    srv.server().stop();

    // Раньше потоки соединений отсоединялись (detach) и могли пережить
    // и сервер, и базу. Теперь run() дожидается всех и возвращается.
    REQUIRE(srv.waitStopped(std::chrono::seconds(10)));
}

TEST_CASE("слишком длинная строка без перевода строки отклоняется", "[server]") {
    RunningServer srv;
    Client client(srv.port());

    // Отправляем в отдельном потоке: сервер перестанет читать, и при
    // заполнении буферов send() заблокируется, пока мы не прочтём ответ.
    const std::string huge(2u << 20, 'x');
    std::thread sender([&] { (void)net::send_all(client.raw(), huge); });

    std::string response;
    const bool got = client.readLine(response);

    // Закрываем нашу сторону, чтобы отправка в потоке гарантированно завершилась.
    net::shutdown_send(client.raw());
#ifdef _WIN32
    ::shutdown(client.raw(), SD_BOTH);
#else
    ::shutdown(client.raw(), SHUT_RDWR);
#endif
    sender.join();

    REQUIRE(got);
    REQUIRE(response == "ERR_COMMAND_TOO_LONG");
}
