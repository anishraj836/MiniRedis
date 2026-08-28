#pragma once

#include "storage/value_object.hpp"
#include "storage/lru_cache.hpp"
#include "storage/ttl_manager.hpp"
#include "storage/memory_tracker.hpp"
#include "common/spinlock.hpp"
#include "common/types.hpp"
#include <shared_mutex>
#include <unordered_map>
#include <string>
#include <optional>
#include <vector>
#include <functional>

namespace miniredis {

class StorageShard {
public:
    explicit StorageShard(MemoryTracker* mem_tracker = nullptr);
    ~StorageShard() = default;

    StorageShard(const StorageShard&) = delete;
    StorageShard& operator=(const StorageShard&) = delete;

    // Mutex direct access for multi-shard lock ordering
    std::shared_mutex& mutex() noexcept { return mutex_; }

    // Core String / Generic Operations (Exclusive Locks)
    bool set(const std::string& key, ValueObject value);
    bool set_string(const std::string& key, std::string val, int64_t expire_at_ms = -1);
    std::optional<std::string> get_string(const std::string& key);
    bool del(const std::string& key);
    bool exists(const std::string& key);
    std::optional<RedisType> type(const std::string& key);

    // TTL Operations
    bool expire(const std::string& key, int64_t expire_at_ms);
    int64_t ttl_ms(const std::string& key);
    bool persist(const std::string& key);

    // Hash Operations
    int hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& field_values);
    std::optional<std::string> hget(const std::string& key, const std::string& field);
    bool hdel(const std::string& key, const std::string& field);
    std::optional<std::unordered_map<std::string, std::string>> hgetall(const std::string& key);
    bool hexists(const std::string& key, const std::string& field);
    size_t hlen(const std::string& key);

    // List Operations
    size_t lpush(const std::string& key, const std::vector<std::string>& values);
    size_t rpush(const std::string& key, const std::vector<std::string>& values);
    std::optional<std::string> lpop(const std::string& key);
    std::optional<std::string> rpop(const std::string& key);
    size_t llen(const std::string& key);
    std::vector<std::string> lrange(const std::string& key, int64_t start, int64_t stop);

    // Set Operations
    size_t sadd(const std::string& key, const std::vector<std::string>& members);
    size_t srem(const std::string& key, const std::vector<std::string>& members);
    bool sismember(const std::string& key, const std::string& member);
    std::vector<std::string> smembers(const std::string& key);
    size_t scard(const std::string& key);

    // Eviction and Expiration
    bool evict_one(EvictionPolicy policy);
    size_t sweep_expired(size_t max_keys);
    void sample_and_sweep(size_t sample_count, size_t& sampled, size_t& expired);

    // Direct iteration under read lock for AOF Rewrite / Snapshot
    void iterate_all(const std::function<void(const std::string& key, const ValueObject& val)>& callback);

    [[nodiscard]] size_t key_count() const;
    [[nodiscard]] size_t memory_usage() const;
    void flush();

private:
    // Internal helper (must be called with lock held)
    bool del_locked(const std::string& key);
    bool check_and_purge_expired_locked(const std::string& key, int64_t now);
    void update_memory_delta(ssize_t delta_bytes);
    bool try_evict_for_space(size_t required_bytes);

    mutable std::shared_mutex mutex_;
    SpinLock lru_lock_; // Internal spinlock for LRU touch under shared_lock
    std::unordered_map<std::string, ValueObject> data_;
    LRUCache lru_;
    TTLManager ttl_manager_;
    size_t shard_memory_{0};
    MemoryTracker* mem_tracker_{nullptr};
};

} // namespace miniredis
