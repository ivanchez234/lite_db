#pragma once

#include "metrics.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>


class BlockCache {
public:
    using Rows = std::shared_ptr<const std::vector<char>>;

    explicit BlockCache(size_t capacity_bytes = 0);

    BlockCache(const BlockCache&) = delete;
    BlockCache& operator=(const BlockCache&) = delete;

    // 0 — кеш выключен.
    void set_capacity(size_t bytes);
    size_t capacity() const;

    // nullptr — блока в кеше нет.
    Rows get(const std::string& file, int64_t offset);
    void put(const std::string& file, int64_t offset, Rows rows);
    void clear();

    uint64_t hits() const noexcept { return hits_.get(); }
    uint64_t misses() const noexcept { return misses_.get(); }

private:
    static constexpr size_t kShards = 8;

    struct Entry {
        std::string key;
        Rows rows;
        size_t charge;   
    };

    struct alignas(64) Shard {
        mutable std::mutex mutex;
        std::list<Entry> lru;  
        std::unordered_map<std::string, std::list<Entry>::iterator> index;
        size_t bytes = 0;
        size_t capacity = 0;

        void evict_to_capacity();
    };

    static std::string make_key(const std::string& file, int64_t offset);
    Shard& shard_for(const std::string& key);

    std::array<Shard, kShards> shards_;
    lite_db::Counter hits_;
    lite_db::Counter misses_;
};
