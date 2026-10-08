#pragma once

#include <atomic>
#include <cstdint>

namespace lite_db {

// Счётчик для статистики (команда STATS).
//
// memory_order_relaxed: нужно только итоговое число, а не порядок относительно
// других операций, поэтому никаких барьеров. Счётчики увеличиваются в основном
// под замком таблицы или журнала, так что за одну кеш-линию соревнуются редко.
class Counter {
public:
    void add(uint64_t n = 1) noexcept { value_.fetch_add(n, std::memory_order_relaxed); }
    uint64_t get() const noexcept { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> value_{0};
};

} // namespace lite_db
