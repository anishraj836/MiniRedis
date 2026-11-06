#include "storage/ttl_manager.hpp"

namespace miniredis {

void TTLManager::set_expiry(const std::string& key, int64_t expire_at_ms) {
    expiry_map_[key] = expire_at_ms;
    min_heap_.push({expire_at_ms, key});
    compact_if_needed();
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

void TTLManager::pop_stale_top() {
    while (!min_heap_.empty()) {
        const auto& top = min_heap_.top();
        auto it = expiry_map_.find(top.key);
        if (it != expiry_map_.end() && it->second == top.expire_at_ms) {
            return; // top is live
        }
        min_heap_.pop();
    }
}

size_t TTLManager::sweep_expired(int64_t current_ms, size_t max_keys, std::vector<std::string>& expired_keys) {
    size_t swept = 0;
    while (swept < max_keys) {
        pop_stale_top();
        if (min_heap_.empty() || min_heap_.top().expire_at_ms > current_ms) {
            break; // Min-heap root not yet expired, so nothing else is
        }

        std::string key = min_heap_.top().key;
        min_heap_.pop();
        expiry_map_.erase(key);
        expired_keys.push_back(std::move(key));
        ++swept;
    }
    return swept;
}

std::optional<std::string> TTLManager::soonest_key() {
    pop_stale_top();
    if (min_heap_.empty()) return std::nullopt;
    return min_heap_.top().key;
}

void TTLManager::compact_if_needed() {
    if (min_heap_.size() <= 2 * expiry_map_.size() + 1024) {
        return;
    }
    std::vector<ExpiryEntry> live;
    live.reserve(expiry_map_.size());
    for (const auto& [key, exp] : expiry_map_) {
        live.push_back({exp, key});
    }
    min_heap_ = decltype(min_heap_)(std::greater<ExpiryEntry>(), std::move(live));
}

} // namespace miniredis
