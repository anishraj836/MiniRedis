#pragma once

#include "storage/storage_shard.hpp"
#include "storage/memory_tracker.hpp"
#include "common/types.hpp"
#include <vector>
#include <memory>
#include <string>
#include <string_view>
#include <functional>
#include <set>

namespace miniredis {

class StorageEngine {
public:
    explicit StorageEngine(size_t shard_count = DEFAULT_SHARD_COUNT,
                           size_t max_memory = DEFAULT_MAX_MEMORY,
                           EvictionPolicy policy = EvictionPolicy::ALLKEYS_LRU);
    ~StorageEngine() = default;

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;

    [[nodiscard]] size_t shard_count() const noexcept { return shard_count_; }
    [[nodiscard]] StorageShard& get_shard(std::string_view key) noexcept {
        size_t idx = hash_key(key) % shard_count_;
        return *shards_[idx];
    }

    [[nodiscard]] StorageShard& get_shard_by_index(size_t index) noexcept {
        return *shards_[index % shard_count_];
    }

    // Direct String operations
    bool set_string(const std::string& key, std::string val, int64_t expire_at_ms = -1) {
        return get_shard(key).set_string(key, std::move(val), expire_at_ms);
    }

    std::optional<std::string> get_string(const std::string& key) {
        return get_shard(key).get_string(key);
    }

    bool del(const std::string& key) {
        return get_shard(key).del(key);
    }

    bool exists(const std::string& key) {
        return get_shard(key).exists(key);
    }

    std::optional<RedisType> type(const std::string& key) {
        return get_shard(key).type(key);
    }

    // TTL operations
    bool expire(const std::string& key, int64_t expire_at_ms) {
        return get_shard(key).expire(key, expire_at_ms);
    }

    int64_t ttl_ms(const std::string& key) {
        return get_shard(key).ttl_ms(key);
    }

    bool persist(const std::string& key) {
        return get_shard(key).persist(key);
    }

    // Multi-key atomic operations with deadlock-free sorted shard locking
    std::vector<std::optional<std::string>> mget(const std::vector<std::string>& keys);
    bool mset(const std::vector<std::pair<std::string, std::string>>& key_values);

    // Active TTL Sweeper (called periodically, e.g. every 100ms)
    // Returns total number of purged keys
    size_t active_expire_cycle(size_t sample_size = 20, double threshold = 0.25);

    // Snapshot iteration across all shards for BGREWRITEAOF
    void snapshot_iterate(const std::function<void(const std::string& key, const ValueObject& val)>& callback);

    [[nodiscard]] size_t total_keys() const;
    [[nodiscard]] size_t total_memory_bytes() const;
    void flush_all();

    [[nodiscard]] MemoryTracker& memory_tracker() noexcept { return mem_tracker_; }
    [[nodiscard]] const MemoryTracker& memory_tracker() const noexcept { return mem_tracker_; }

private:
    size_t shard_count_;
    MemoryTracker mem_tracker_;
    std::vector<std::unique_ptr<StorageShard>> shards_;
};

} // namespace miniredis
