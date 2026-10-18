#pragma once
#include <atomic>
#include <cstdint>

namespace lite_db {

class Counter {
public:
    void add(uint64_t n = 1) noexcept { value_.fetch_add(n, std::memory_order_relaxed); }
    uint64_t get() const noexcept { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> value_{0};
};

} // namespace lite_db
