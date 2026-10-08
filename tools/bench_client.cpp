// Нагрузочный клиент lite_db: задержки по перцентилям и пропускная способность.
//
//   bench_client --workload insert --clients 4 --ops 5000
//   bench_client --workload select --clients 4 --ops 20000 --preload 100000
//
// Каждый клиент — отдельный поток со своим соединением, работающий в режиме
// «запрос — ответ». Задержка измеряется на каждом запросе, перцентили
// считаются по всем запросам всех клиентов.
//
// Нагрузки:
//   insert  — вставка новых записей (--batch N: по N записей командой MPUT)
//   select  — чтение случайных записей из предварительно загруженных (--preload)
//   update  — частичное обновление случайных загруженных записей
//
// --pipeline K: отправлять K запросов одной пачкой и только потом читать
// K ответов (конвейер). Задержка меряется на пачку.
//
// --table-per-client 1: у каждого клиента своя таблица (bench_0, bench_1, ...),
// чтобы записи не соревновались за замок одной таблицы.
//
// --json файл: дописать результат одной строкой JSON (для скриптов).

#include "server/socket_handle.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    std::string host     = "127.0.0.1";
    int         port     = 5555;
    std::string workload = "insert";
    std::string table    = "bench";
    int         clients  = 1;
    int         ops      = 1000;     // запросов на клиента
    int         batch    = 1;        // записей в одном запросе (insert)
    int         preload  = 0;        // записей загрузить перед select/update
    long long   id_base  = 0;        // с какого id начинать вставку
    bool        table_per_client = false;
    int         pipeline = 1;
    std::string label;               // подпись результата
    std::string json_out;
};

class Connection {
public:
    bool connect(const std::string& host, int port) {
        sock_ = net::SocketHandle(::socket(AF_INET, SOCK_STREAM, 0));
        if (!sock_.valid()) return false;
        net::set_no_delay(sock_.get());
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(static_cast<unsigned short>(port));
        if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) return false;
        return ::connect(sock_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    }

    bool request(const std::string& command, std::string& reply) {
        return net::send_all(sock_.get(), command + "\n") && read_reply(reply);
    }

    bool send_raw(const std::string& data) { return net::send_all(sock_.get(), data); }

    bool read_reply(std::string& reply) {
        for (;;) {
            const size_t newline = buffered_.find('\n');
            if (newline != std::string::npos) {
                reply = buffered_.substr(0, newline);
                buffered_.erase(0, newline + 1);
                return true;
            }
            char chunk[65536];
            const auto received = ::recv(sock_.get(), chunk, static_cast<int>(sizeof(chunk)), 0);
            if (received <= 0) return false;
            buffered_.append(chunk, static_cast<size_t>(received));
        }
    }

private:
    net::SocketHandle sock_;
    std::string       buffered_;
};

std::string row_json(long long id) {
    return "{\"name\":\"user" + std::to_string(id) + "\",\"age\":" + std::to_string(id % 90)
         + ",\"active\":" + ((id % 2) ? "true" : "false") + "}";
}

std::string table_for(const Options& o, int client) {
    return o.table_per_client ? o.table + "_" + std::to_string(client) : o.table;
}

std::string insert_command(const std::string& table, long long first_id, int count) {
    if (count == 1) return "INSERT " + table + " " + std::to_string(first_id) + " " + row_json(first_id);
    std::string cmd = "MPUT " + table;
    for (int i = 0; i < count; ++i) {
        cmd += ' ';
        cmd += std::to_string(first_id + i);
        cmd += ' ';
        cmd += row_json(first_id + i);
    }
    return cmd;
}

bool is_ok(const std::string& reply) { return reply.rfind("OK", 0) == 0; }

struct ClientResult {
    std::vector<uint32_t> latencies_us;
    long long             errors = 0;
    std::string           first_error;
};

void run_client(const Options& o, int index, ClientResult& out) {
    Connection conn;
    if (!conn.connect(o.host, o.port)) {
        out.errors = o.ops;
        out.first_error = "connect failed";
        return;
    }
    out.latencies_us.reserve(static_cast<size_t>(o.ops));

    std::mt19937_64 rng(0x5eedULL + static_cast<uint64_t>(index));
    std::uniform_int_distribution<long long> pick(0, std::max(0, o.preload - 1));

    const std::string table = table_for(o, index);
    std::string reply;
    for (int op = 0; op < o.ops; ++op) {
        std::string cmd;
        if (o.workload == "insert") {
            const long long first = o.id_base + (static_cast<long long>(index) * o.ops + op) * o.batch;
            cmd = insert_command(table, first, o.batch);
        } else if (o.workload == "select") {
            cmd = "SELECT " + table + " " + std::to_string(pick(rng));
        } else {  // update
            const long long id = pick(rng);
            cmd = "UPDATE " + table + " " + std::to_string(id) + " {\"age\":" + std::to_string(op % 90) + "}";
        }

        const auto start = Clock::now();
        bool sent = true;
        if (o.pipeline <= 1) {
            sent = conn.request(cmd, reply);
        } else {
            // Конвейер: K копий команды одной отправкой, затем K ответов.
            std::string batch;
            for (int k = 0; k < o.pipeline; ++k) batch += cmd + "\n";
            sent = conn.send_raw(batch);
            for (int k = 0; sent && k < o.pipeline; ++k) sent = conn.read_reply(reply);
        }
        const auto took = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();

        if (!sent) {
            out.errors += o.ops - op;
            if (out.first_error.empty()) out.first_error = "connection lost";
            return;
        }
        const bool ok = o.workload == "select" ? reply.front() == '{' : is_ok(reply);
        if (!ok) {
            ++out.errors;
            if (out.first_error.empty()) out.first_error = reply.substr(0, 120);
        }
        out.latencies_us.push_back(static_cast<uint32_t>(took));
    }
}

bool parse_args(int argc, char** argv, Options& o) {
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string key = argv[i];
        const std::string val = argv[i + 1];
        if (key == "--host") o.host = val;
        else if (key == "--port") o.port = std::stoi(val);
        else if (key == "--workload") o.workload = val;
        else if (key == "--table") o.table = val;
        else if (key == "--clients") o.clients = std::stoi(val);
        else if (key == "--ops") o.ops = std::stoi(val);
        else if (key == "--batch") o.batch = std::stoi(val);
        else if (key == "--preload") o.preload = std::stoi(val);
        else if (key == "--id-base") o.id_base = std::stoll(val);
        else if (key == "--table-per-client") o.table_per_client = (val == "1");
        else if (key == "--pipeline") o.pipeline = std::stoi(val);
        else if (key == "--label") o.label = val;
        else if (key == "--json") o.json_out = val;
        else return false;
    }
    if (o.workload != "insert" && o.workload != "select" && o.workload != "update") return false;
    if ((o.workload != "insert") && o.preload <= 0) {
        std::cerr << "select/update need --preload N" << std::endl;
        return false;
    }
    return o.clients > 0 && o.ops > 0 && o.batch > 0;
}

// Загрузка данных для select/update: пачками, затем FLUSH, чтобы записи
// лежали в сжатых блоках на диске, а не в буфере в памяти.
bool preload(const Options& o) {
    Connection conn;
    if (!conn.connect(o.host, o.port)) return false;
    std::string reply;
    const int chunk = 500;
    const int tables = o.table_per_client ? o.clients : 1;
    for (int t = 0; t < tables; ++t) {
        const std::string table = table_for(o, t);
        for (int id = 0; id < o.preload; id += chunk) {
            const int count = std::min(chunk, o.preload - id);
            if (!conn.request(insert_command(table, id, count), reply)) return false;
            if (is_ok(reply)) continue;
            // Сервер без MPUT — по одной записи.
            for (int k = 0; k < count; ++k) {
                if (!conn.request(insert_command(table, id + k, 1), reply)) return false;
            }
        }
    }
    return conn.request("FLUSH", reply) && is_ok(reply);
}

uint32_t percentile(const std::vector<uint32_t>& sorted, double p) {
    if (sorted.empty()) return 0;
    const size_t idx = std::min(sorted.size() - 1, static_cast<size_t>(std::lround(p / 100.0 * static_cast<double>(sorted.size() - 1))));
    return sorted[idx];
}

} // namespace

int main(int argc, char** argv) {
    Options o;
    if (!parse_args(argc, argv, o)) {
        std::cerr << "usage: bench_client --workload insert|select|update [--clients N] [--ops N]\n"
                     "                    [--batch N] [--preload N] [--table T] [--host H] [--port P]\n"
                     "                    [--id-base N] [--label TEXT] [--json FILE]" << std::endl;
        return 2;
    }

#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) return 1;
#endif

    {
        Connection setup;
        std::string reply;
        if (!setup.connect(o.host, o.port)) {
            std::cerr << "cannot connect to " << o.host << ":" << o.port << std::endl;
            return 1;
        }
        const int tables = o.table_per_client ? o.clients : 1;
        for (int t = 0; t < tables; ++t) {
            setup.request("CREATE " + table_for(o, t), reply);
            setup.request("SCHEMA " + table_for(o, t) + " name:STRING age:INT active:BOOL", reply);
        }
    }

    if (o.workload != "insert" && !preload(o)) {
        std::cerr << "preload failed" << std::endl;
        return 1;
    }

    std::vector<ClientResult> results(static_cast<size_t>(o.clients));
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(o.clients));

    const auto start = Clock::now();
    for (int c = 0; c < o.clients; ++c) {
        threads.emplace_back(run_client, std::cref(o), c, std::ref(results[static_cast<size_t>(c)]));
    }
    for (auto& t : threads) t.join();
    const double seconds = std::chrono::duration<double>(Clock::now() - start).count();

    std::vector<uint32_t> all;
    long long errors = 0;
    std::string first_error;
    for (auto& r : results) {
        all.insert(all.end(), r.latencies_us.begin(), r.latencies_us.end());
        errors += r.errors;
        if (first_error.empty()) first_error = r.first_error;
    }
    std::sort(all.begin(), all.end());

    // В режиме конвейера одна замеренная задержка — это пачка из K запросов.
    const double requests = static_cast<double>(all.size()) * o.pipeline;
    const double rows = requests * (o.workload == "insert" ? o.batch : 1);

    char summary[512];
    (void)std::snprintf(summary, sizeof(summary),
                  "%-8s clients=%d batch=%d pipeline=%d  requests=%.0f  %.0f req/s  %.0f rows/s  "
                  "p50=%uus p90=%uus p99=%uus p99.9=%uus max=%uus  errors=%lld",
                  o.workload.c_str(), o.clients, o.batch, o.pipeline, requests, requests / seconds, rows / seconds,
                  percentile(all, 50), percentile(all, 90), percentile(all, 99), percentile(all, 99.9),
                  all.empty() ? 0u : all.back(), errors);
    std::cout << (o.label.empty() ? "" : o.label + ": ") << summary << std::endl;
    if (errors > 0) std::cout << "first error: " << first_error << std::endl;

    if (!o.json_out.empty()) {
        std::ofstream json(o.json_out, std::ios::app);
        json << "{\"label\":\"" << o.label << "\",\"workload\":\"" << o.workload
             << "\",\"clients\":" << o.clients << ",\"batch\":" << o.batch
             << ",\"table_per_client\":" << (o.table_per_client ? "true" : "false")
             << ",\"pipeline\":" << o.pipeline
             << ",\"requests\":" << all.size() << ",\"seconds\":" << seconds
             << ",\"req_per_s\":" << requests / seconds << ",\"rows_per_s\":" << rows / seconds
             << ",\"p50_us\":" << percentile(all, 50) << ",\"p90_us\":" << percentile(all, 90)
             << ",\"p99_us\":" << percentile(all, 99) << ",\"p999_us\":" << percentile(all, 99.9)
             << ",\"max_us\":" << (all.empty() ? 0u : all.back()) << ",\"errors\":" << errors << "}\n";
    }

#ifdef _WIN32
    WSACleanup();
#endif
    return errors == 0 ? 0 : 1;
}
