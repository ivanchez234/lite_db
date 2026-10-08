// Замер движка хранения без сети и журнала: как размер блока влияет
// на сжатие, скорость чтения с диска и стоимость сброса блока.
//
//   bench_storage [--rows N] [--reads N] [--dir PATH]
//
// Для каждого размера блока: вставляет N записей (время каждой вставки —
// в ней же сжимается и пишется блок, когда буфер заполнится), сбрасывает
// всё на диск, затем читает случайные записи без кеша и с кешем.
// Данные пишутся во временный каталог (--dir, по умолчанию bench_storage_data)
// и удаляются после замера.

#include "storage/storage.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

double us_since(Clock::time_point start) {
    return std::chrono::duration<double, std::micro>(Clock::now() - start).count();
}

double percentile(std::vector<double>& v, double p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, static_cast<size_t>(std::lround(p / 100.0 * static_cast<double>(v.size() - 1))))];
}

// Те же записи, что у bench_client: повторяющиеся имена и небольшие числа —
// типичные «табличные» данные, которые хорошо сжимаются.
std::string row_json(int id) {
    return "{\"name\":\"user" + std::to_string(id) + "\",\"age\":" + std::to_string(id % 90)
         + ",\"active\":" + ((id % 2) ? "true" : "false") + "}";
}

struct Result {
    int    block_kb;
    double ratio;
    double insert_p50, insert_p99, insert_max;
    double read_cold_p50, read_cold_p99;
    double read_cached_p50, read_cached_p99;
};

Result run(int block_kb, int rows, int reads, const std::string& dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);

    Result r{};
    r.block_kb = block_kb;

    Storage st(dir);
    st.set_block_size(static_cast<size_t>(block_kb) * 1024u);
    st.set_block_cache_bytes(0);
    st.create_table("bench");
    st.set_schema("bench", {{"name", DataType::STRING}, {"age", DataType::INT}, {"active", DataType::BOOL}});

    std::vector<double> inserts;
    inserts.reserve(static_cast<size_t>(rows));
    for (int id = 0; id < rows; ++id) {
        const std::string json = row_json(id);
        const auto start = Clock::now();
        st.insert("bench", id, json);
        inserts.push_back(us_since(start));
    }
    if (!st.flush_all()) std::cerr << "flush failed" << std::endl;

    const auto& s = st.stats();
    r.ratio = static_cast<double>(s.block_bytes_raw.get()) / static_cast<double>(s.block_bytes_compressed.get());
    r.insert_p50 = percentile(inserts, 50);
    r.insert_p99 = percentile(inserts, 99);
    r.insert_max = inserts.back();

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> pick(0, rows - 1);

    std::vector<double> cold;
    for (int i = 0; i < reads; ++i) {
        const int id = pick(rng);
        const auto start = Clock::now();
        const std::string got = st.select("bench", id);
        cold.push_back(us_since(start));
        if (got.front() != '{') std::cerr << "read failed: " << got << std::endl;
    }

    st.set_block_cache_bytes(256u * 1024u * 1024u);
    for (int id = 0; id < rows; ++id) st.select("bench", id);   // прогрев: весь набор в кеше
    std::vector<double> cached;
    for (int i = 0; i < reads; ++i) {
        const int id = pick(rng);
        const auto start = Clock::now();
        st.select("bench", id);
        cached.push_back(us_since(start));
    }

    r.read_cold_p50 = percentile(cold, 50);
    r.read_cold_p99 = percentile(cold, 99);
    r.read_cached_p50 = percentile(cached, 50);
    r.read_cached_p99 = percentile(cached, 99);

    std::filesystem::remove_all(dir, ec);
    return r;
}

} // namespace

int main(int argc, char** argv) {
    int rows = 100000;
    int reads = 20000;
    std::string dir = "bench_storage_data";
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string key = argv[i];
        if (key == "--rows") rows = std::stoi(argv[i + 1]);
        else if (key == "--reads") reads = std::stoi(argv[i + 1]);
        else if (key == "--dir") dir = argv[i + 1];
    }

    std::cout << "rows=" << rows << " reads=" << reads << "\n\n"
              << "| Блок | Сжатие | Вставка p50 / p99 / max, мкс | Чтение без кеша p50 / p99, мкс | Чтение из кеша p50 / p99, мкс |\n"
              << "|---:|---:|---|---|---|\n";

    for (int kb : {4, 16, 64, 256}) {
        const Result r = run(kb, rows, reads, dir);
        char line[256];
        (void)std::snprintf(line, sizeof(line),
                      "| %d КБ | %.1f× | %.1f / %.1f / %.0f | %.1f / %.1f | %.1f / %.1f |",
                      r.block_kb, r.ratio, r.insert_p50, r.insert_p99, r.insert_max,
                      r.read_cold_p50, r.read_cold_p99, r.read_cached_p50, r.read_cached_p99);
        std::cout << line << std::endl;
    }
    return 0;
}
