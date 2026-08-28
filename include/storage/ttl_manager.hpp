#pragma once

#include <string>
#include <unordered_map>
#include <queue>
#include <vector>
#include <cstdint>
#include <optional>
#include <random>

namespace miniredis {

struct ExpiryEntry {
    int64_t expire_at_ms;
    std::string key;

    bool operator>(const ExpiryEntry& other) const noexcept {
        return expire_at_ms > other.expire_at_ms;
    }
};

class TTLManager {
public:
    TTLManager() = default;
    ~TTLManager() = default;

    void set_expiry(const std::string& key, int64_t expire_at_ms);
    void remove_expiry(const std::string& key);
    [[nodiscard]] std::optional<int64_t> get_expiry(const std::string& key) const;

    // Sweeps up to max_keys that have expired before current_ms
    size_t sweep_expired(int64_t current_ms, size_t max_keys, std::vector<std::string>& expired_keys);

    // Probabilistic sampling for active Redis-style sweeping
    void sample_keys(size_t count, std::vector<std::pair<std::string, int64_t>>& sampled);

    [[nodiscard]] size_t size() const noexcept {
        return expiry_map_.size();
    }

    [[nodiscard]] bool empty() const noexcept {
        return expiry_map_.empty();
    }

    void clear() noexcept {
        expiry_map_.clear();
        while (!min_heap_.empty()) min_heap_.pop();
    }

private:
    std::unordered_map<std::string, int64_t> expiry_map_;
    std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> min_heap_;
};

} // namespace miniredis
