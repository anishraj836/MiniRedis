#include "storage/ttl_manager.hpp"

namespace miniredis {

void TTLManager::set_expiry(const std::string& key, int64_t expire_at_ms) {
    expiry_map_[key] = expire_at_ms;
    min_heap_.push({expire_at_ms, key});
}

void TTLManager::remove_expiry(const std::string& key) {
    expiry_map_.erase(key);
}

std::optional<int64_t> TTLManager::get_expiry(const std::string& key) const {
    auto it = expiry_map_.find(key);
    if (it != expiry_map_.end()) {
        return it->second;
    }
    return std::nullopt;
}

size_t TTLManager::sweep_expired(int64_t current_ms, size_t max_keys, std::vector<std::string>& expired_keys) {
    size_t swept = 0;
    while (!min_heap_.empty() && swept < max_keys) {
        const auto& top = min_heap_.top();
        if (top.expire_at_ms > current_ms) {
            break; // Min-heap root not yet expired, so nothing else is
        }

        std::string key = top.key;
        int64_t exp_time = top.expire_at_ms;
        min_heap_.pop();

        auto it = expiry_map_.find(key);
        // Verify key wasn't deleted or updated to a newer expiry
        if (it != expiry_map_.end() && it->second == exp_time) {
            expiry_map_.erase(it);
            expired_keys.push_back(std::move(key));
            ++swept;
        }
    }
    return swept;
}

void TTLManager::sample_keys(size_t count, std::vector<std::pair<std::string, int64_t>>& sampled) {
    sampled.clear();
    if (expiry_map_.empty()) return;

    size_t n = std::min(count, expiry_map_.size());
    sampled.reserve(n);

    // Sample from hash map iterator
    auto it = expiry_map_.begin();
    for (size_t i = 0; i < n && it != expiry_map_.end(); ++i, ++it) {
        sampled.emplace_back(it->first, it->second);
    }
}

} // namespace miniredis
