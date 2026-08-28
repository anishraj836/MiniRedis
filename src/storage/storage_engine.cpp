#include "storage/storage_engine.hpp"

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
    std::vector<std::optional<std::string>> results;
    results.reserve(keys.size());

    for (const auto& key : keys) {
        results.push_back(get_string(key));
    }
    return results;
}

bool StorageEngine::mset(const std::vector<std::pair<std::string, std::string>>& key_values) {
    // Sort unique shard indices to prevent deadlock
    std::set<size_t> unique_shard_indices;
    for (const auto& [k, v] : key_values) {
        unique_shard_indices.insert(hash_key(k) % shard_count_);
    }

    // Set each key-value pair
    for (const auto& [k, v] : key_values) {
        if (!set_string(k, v)) {
            return false;
        }
    }
    return true;
}

size_t StorageEngine::active_expire_cycle(size_t sample_size, double threshold) {
    size_t total_expired = 0;

    for (size_t i = 0; i < shard_count_; ++i) {
        auto& shard = *shards_[i];
        while (true) {
            size_t sampled = 0;
            size_t expired = 0;
            shard.sample_and_sweep(sample_size, sampled, expired);
            total_expired += expired;

            if (sampled == 0 || (static_cast<double>(expired) / static_cast<double>(sampled)) < threshold) {
                break; // Ratio of expired keys is below threshold (25%), move to next shard
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

size_t StorageEngine::total_memory_bytes() const {
    return mem_tracker_.get_used_memory();
}

void StorageEngine::flush_all() {
    for (size_t i = 0; i < shard_count_; ++i) {
        shards_[i]->flush();
    }
}

} // namespace miniredis
