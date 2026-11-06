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

    // Key with the earliest expiry (does not remove it), skipping stale heap entries.
    [[nodiscard]] std::optional<std::string> soonest_key();

    [[nodiscard]] size_t heap_size() const noexcept {
        return min_heap_.size();
    }

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
    // The heap uses lazy deletion: updating or removing a TTL leaves the old
    // entry behind. Rebuild it when stale entries dominate, so refreshing TTLs
    // in a loop cannot grow the heap without bound.
    void compact_if_needed();
    void pop_stale_top();

    std::unordered_map<std::string, int64_t> expiry_map_;
    std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> min_heap_;
};

} // namespace miniredis
