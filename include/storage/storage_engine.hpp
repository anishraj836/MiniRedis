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
    bool set_string(const std::string& key, std::string val, int64_t expire_at_ms = -1, bool keep_ttl = false) {
        return get_shard(key).set_string(key, std::move(val), expire_at_ms, keep_ttl);
    }

    std::optional<std::string> get_string(const std::string& key) {
        return get_shard(key).get_string(key);
    }

    LookupStatus get_string_checked(const std::string& key, std::string& out) {
        return get_shard(key).get_string_checked(key, out);
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

    // Multi-key atomic operations with deadlock-free sorted shard locking.
    // MSET is all-or-nothing: on OOM nothing is written.
    std::vector<std::optional<std::string>> mget(const std::vector<std::string>& keys);
    bool mset(const std::vector<std::pair<std::string, std::string>>& key_values);

    // Active TTL Sweeper (called periodically, e.g. every 100ms).
    // Pops expired keys from each shard's min-heap in batches, soonest first,
    // and stops when the time budget is used up (the next cycle resumes at the
    // following shard). Returns total number of purged keys.
    size_t active_expire_cycle(size_t batch_size = 20, int64_t time_budget_us = 2000);

    // Snapshot iteration across all shards for BGREWRITEAOF
    void snapshot_iterate(const std::function<void(const std::string& key, const ValueObject& val)>& callback);

    [[nodiscard]] size_t total_keys() const;
    [[nodiscard]] size_t total_expires() const;
    [[nodiscard]] size_t total_memory_bytes() const;
    [[nodiscard]] bool verify_memory_accounting() const;
    void flush_all();

    [[nodiscard]] MemoryTracker& memory_tracker() noexcept { return mem_tracker_; }
    [[nodiscard]] const MemoryTracker& memory_tracker() const noexcept { return mem_tracker_; }

private:
    [[nodiscard]] size_t shard_index(std::string_view key) const noexcept {
        return hash_key(key) % shard_count_;
    }

    size_t shard_count_;
    MemoryTracker mem_tracker_;
    std::vector<std::unique_ptr<StorageShard>> shards_;
    size_t next_expire_shard_{0}; // only touched by the thread running the expire cycle
};

} // namespace miniredis
