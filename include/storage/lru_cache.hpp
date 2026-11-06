#pragma once

#include <list>
#include <unordered_map>
#include <string>
#include <optional>
#include <cstddef>

namespace miniredis {

class LRUCache {
public:
    LRUCache() = default;
    ~LRUCache() = default;

    LRUCache(const LRUCache&) = delete;
    LRUCache& operator=(const LRUCache&) = delete;

    // Fast O(1) key promotion to front (Most Recently Used)
    void touch(const std::string& key);

    // Insert new key at front
    void insert(const std::string& key);

    // Erase key
    void erase(const std::string& key);

    // Evict Least Recently Used key
    [[nodiscard]] std::optional<std::string> pop_lru();

    // Peek Least Recently Used key
    [[nodiscard]] std::optional<std::string> peek_lru() const;

    // Returns the least recently used key matching `pred`, looking at no more
    // than `max_scan` keys from the LRU end.
    template <typename Pred>
    [[nodiscard]] std::optional<std::string> find_lru_if(Pred pred, size_t max_scan) const {
        size_t scanned = 0;
        for (auto it = key_list_.rbegin(); it != key_list_.rend() && scanned < max_scan; ++it, ++scanned) {
            if (pred(*it)) {
                return *it;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] size_t size() const noexcept {
        return key_map_.size();
    }

    [[nodiscard]] bool empty() const noexcept {
        return key_map_.empty();
    }

    void clear() noexcept {
        key_list_.clear();
        key_map_.clear();
    }

private:
    std::list<std::string> key_list_; // Front = MRU, Back = LRU
    std::unordered_map<std::string, std::list<std::string>::iterator> key_map_;
};

} // namespace miniredis
