#include "storage/storage_engine.hpp"
#include <map>
#include <mutex>
#include <shared_mutex>

namespace miniredis {

StorageEngine::StorageEngine(size_t shard_count, size_t max_memory, EvictionPolicy policy)
    : shard_count_(shard_count > 0 ? shard_count : DEFAULT_SHARD_COUNT),
      mem_tracker_(max_memory, policy) {
    shards_.reserve(shard_count_);
    for (size_t i = 0; i < shard_count_; ++i) {
        shards_.push_back(std::make_unique<StorageShard>(&mem_tracker_));
    }
}

std::vector<std::optional<std::string>> StorageEngine::mget(const std::vector<std::string>& keys) {
    // Shared-lock every involved shard in ascending index order (the global
    // lock order, so this cannot deadlock with MSET) to read one consistent
    // snapshot: a concurrent MSET is seen either entirely or not at all.
    std::set<size_t> indices;
    for (const auto& key : keys) indices.insert(shard_index(key));

    std::vector<std::shared_lock<std::shared_mutex>> locks;
    locks.reserve(indices.size());
    for (size_t idx : indices) locks.emplace_back(shards_[idx]->mutex());

    int64_t now = now_ms();
    std::vector<std::optional<std::string>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.push_back(shards_[shard_index(key)]->get_string_shared_locked(key, now));
    }
    return results;
}

bool StorageEngine::mset(const std::vector<std::pair<std::string, std::string>>& key_values) {
    // Group writes by shard; std::map iterates in ascending shard order, which
    // is the global lock order.
    std::map<size_t, std::vector<const std::pair<std::string, std::string>*>> by_shard;
    for (const auto& kv : key_values) by_shard[shard_index(kv.first)].push_back(&kv);

    std::vector<std::unique_lock<std::shared_mutex>> locks;
    locks.reserve(by_shard.size());
    for (const auto& [idx, _] : by_shard) locks.emplace_back(shards_[idx]->mutex());

    // Phase 1: admission. Make room for every new value before writing any, so
    // an OOM leaves the database untouched instead of half-updated. The check
    // is cumulative: each shard must fit its own values on top of everything
    // the earlier shards are about to add.
    size_t pending = 0;
    for (const auto& [idx, kvs] : by_shard) {
        for (const auto* kv : kvs) {
            pending += sizeof(ValueObject) + 48 + kv->first.size() + kv->second.size() + 2 * sizeof(void*);
        }
        if (!shards_[idx]->reserve_memory_locked(pending)) {
            return false;
        }
    }

    // Phase 2: apply.
    for (const auto& [idx, kvs] : by_shard) {
        for (const auto* kv : kvs) {
            shards_[idx]->set_locked(kv->first, ValueObject::make_string(kv->second), false, true);
        }
    }
    return true;
}

size_t StorageEngine::active_expire_cycle(size_t batch_size, int64_t time_budget_us) {
    if (batch_size == 0) batch_size = 1;
    const int64_t deadline_ns = steady_now_ns() + time_budget_us * 1000;
    size_t total_expired = 0;

    for (size_t visited = 0; visited < shard_count_; ++visited) {
        size_t idx = next_expire_shard_;
        next_expire_shard_ = (next_expire_shard_ + 1) % shard_count_;

        while (true) {
            size_t expired = shards_[idx]->sweep_expired(batch_size);
            total_expired += expired;
            if (expired < batch_size) {
                break; // this shard has nothing more that is due
            }
            if (steady_now_ns() >= deadline_ns) {
                next_expire_shard_ = idx; // resume here next cycle
                return total_expired;
            }
        }
    }
    return total_expired;
}

void StorageEngine::snapshot_iterate(const std::function<void(const std::string& key, const ValueObject& val)>& callback) {
    for (size_t i = 0; i < shard_count_; ++i) {
        shards_[i]->iterate_all(callback);
    }
}

size_t StorageEngine::total_keys() const {
    size_t total = 0;
    for (size_t i = 0; i < shard_count_; ++i) {
        total += shards_[i]->key_count();
    }
    return total;
}

size_t StorageEngine::total_expires() const {
    size_t total = 0;
    for (size_t i = 0; i < shard_count_; ++i) {
        total += shards_[i]->expires_count();
    }
    return total;
}

size_t StorageEngine::total_memory_bytes() const {
    return mem_tracker_.get_used_memory();
}

bool StorageEngine::verify_memory_accounting() const {
    size_t sum = 0;
    for (size_t i = 0; i < shard_count_; ++i) {
        if (!shards_[i]->verify_memory_accounting()) return false;
        sum += shards_[i]->memory_usage();
    }
    return sum == mem_tracker_.get_used_memory();
}

void StorageEngine::flush_all() {
    for (size_t i = 0; i < shard_count_; ++i) {
        shards_[i]->flush();
    }
}

} // namespace miniredis
