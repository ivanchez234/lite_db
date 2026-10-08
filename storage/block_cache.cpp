#include "block_cache.h"

#include <functional>

namespace {

// Накладные расходы на запись кеша сверх самих данных (ключ, узлы списка
// и хеш-таблицы). Точное число не важно — важно, чтобы маленькие блоки
// не считались бесплатными.
constexpr size_t kEntryOverhead = 128;

} // namespace

BlockCache::BlockCache(size_t capacity_bytes) {
    set_capacity(capacity_bytes);
}

void BlockCache::set_capacity(size_t bytes) {
    for (Shard& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.capacity = bytes / kShards;
        shard.evict_to_capacity();
    }
}

size_t BlockCache::capacity() const {
    size_t total = 0;
    for (const Shard& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        total += shard.capacity;
    }
    return total;
}

std::string BlockCache::make_key(const std::string& file, int64_t offset) {
    return file + '#' + std::to_string(offset);
}

BlockCache::Shard& BlockCache::shard_for(const std::string& key) {
    return shards_[std::hash<std::string>{}(key) % kShards];
}

BlockCache::Rows BlockCache::get(const std::string& file, int64_t offset) {
    const std::string key = make_key(file, offset);
    Shard& shard = shard_for(key);

    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto it = shard.index.find(key);
    if (it == shard.index.end()) {
        misses_.add();
        return nullptr;
    }
    // Недавно использованный — в начало списка.
    shard.lru.splice(shard.lru.begin(), shard.lru, it->second);
    hits_.add();
    return it->second->rows;
}

void BlockCache::put(const std::string& file, int64_t offset, Rows rows) {
    if (!rows) return;
    std::string key = make_key(file, offset);
    Shard& shard = shard_for(key);
    const size_t charge = rows->size() + key.size() + kEntryOverhead;

    std::lock_guard<std::mutex> lock(shard.mutex);
    // Блок, который в одиночку не помещается, не кешируем: он вытеснил бы всё.
    if (charge > shard.capacity) return;

    // Два читателя могли одновременно промахнуться и прочитать один блок.
    if (shard.index.count(key) != 0) return;

    shard.lru.push_front(Entry{key, std::move(rows), charge});
    shard.index.emplace(std::move(key), shard.lru.begin());
    shard.bytes += charge;
    shard.evict_to_capacity();
}

void BlockCache::clear() {
    for (Shard& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.index.clear();
        shard.lru.clear();
        shard.bytes = 0;
    }
}

void BlockCache::Shard::evict_to_capacity() {
    while (bytes > capacity && !lru.empty()) {
        const Entry& victim = lru.back();
        bytes -= victim.charge;
        index.erase(victim.key);
        lru.pop_back();
    }
}
