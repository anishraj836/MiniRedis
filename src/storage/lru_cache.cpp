#include "storage/lru_cache.hpp"

namespace miniredis {

void LRUCache::touch(const std::string& key) {
    auto it = key_map_.find(key);
    if (it != key_map_.end()) {
        key_list_.splice(key_list_.begin(), key_list_, it->second);
    }
}

void LRUCache::insert(const std::string& key) {
    auto it = key_map_.find(key);
    if (it != key_map_.end()) {
        key_list_.splice(key_list_.begin(), key_list_, it->second);
    } else {
        key_list_.push_front(key);
        key_map_[key] = key_list_.begin();
    }
}

void LRUCache::erase(const std::string& key) {
    auto it = key_map_.find(key);
    if (it != key_map_.end()) {
        key_list_.erase(it->second);
        key_map_.erase(it);
    }
}

std::optional<std::string> LRUCache::pop_lru() {
    if (key_list_.empty()) {
        return std::nullopt;
    }
    std::string lru_key = std::move(key_list_.back());
    key_list_.pop_back();
    key_map_.erase(lru_key);
    return lru_key;
}

std::optional<std::string> LRUCache::peek_lru() const {
    if (key_list_.empty()) {
        return std::nullopt;
    }
    return key_list_.back();
}

} // namespace miniredis
