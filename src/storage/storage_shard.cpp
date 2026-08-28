#include "storage/storage_shard.hpp"
#include <mutex>
#include <algorithm>

namespace miniredis {

StorageShard::StorageShard(MemoryTracker* mem_tracker)
    : mem_tracker_(mem_tracker) {}

void StorageShard::update_memory_delta(ssize_t delta_bytes) {
    if (delta_bytes > 0) {
        shard_memory_ += delta_bytes;
        if (mem_tracker_) mem_tracker_->add_memory(static_cast<size_t>(delta_bytes));
    } else if (delta_bytes < 0) {
        size_t abs_delta = static_cast<size_t>(-delta_bytes);
        shard_memory_ = (shard_memory_ >= abs_delta) ? (shard_memory_ - abs_delta) : 0;
        if (mem_tracker_) mem_tracker_->sub_memory(abs_delta);
    }
}

bool StorageShard::try_evict_for_space(size_t required_bytes) {
    if (!mem_tracker_ || !mem_tracker_->is_out_of_memory(required_bytes)) {
        return true;
    }

    EvictionPolicy policy = mem_tracker_->get_eviction_policy();
    if (policy == EvictionPolicy::NO_EVICTION) {
        return false;
    }

    while (mem_tracker_->is_out_of_memory(required_bytes) && !data_.empty()) {
        if (!evict_one(policy)) {
            break;
        }
    }
    return !mem_tracker_->is_out_of_memory(required_bytes);
}

bool StorageShard::check_and_purge_expired_locked(const std::string& key, int64_t now) {
    auto it = data_.find(key);
    if (it == data_.end()) return false;

    if (it->second.is_expired(now)) {
        size_t mem = it->second.estimate_memory() + key.capacity() + sizeof(void*);
        update_memory_delta(-static_cast<ssize_t>(mem));
        lru_.erase(key);
        ttl_manager_.remove_expiry(key);
        data_.erase(it);
        return true;
    }
    return false;
}

bool StorageShard::set(const std::string& key, ValueObject value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    check_and_purge_expired_locked(key, now);

    size_t new_mem = value.estimate_memory() + key.capacity() + sizeof(void*);
    size_t old_mem = 0;

    auto it = data_.find(key);
    if (it != data_.end()) {
        old_mem = it->second.estimate_memory() + key.capacity() + sizeof(void*);
    }

    ssize_t delta = static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem);
    if (delta > 0 && !try_evict_for_space(delta)) {
        return false; // Out of memory and eviction failed / noeviction
    }

    if (value.expire_at_ms >= 0) {
        ttl_manager_.set_expiry(key, value.expire_at_ms);
    } else {
        ttl_manager_.remove_expiry(key);
    }

    lru_.insert(key);
    data_[key] = std::move(value);
    update_memory_delta(delta);
    return true;
}

bool StorageShard::set_string(const std::string& key, std::string val, int64_t expire_at_ms) {
    return set(key, ValueObject::make_string(std::move(val), expire_at_ms));
}

std::optional<std::string> StorageShard::get_string(const std::string& key) {
    // Acquire shared lock for concurrent reads
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) {
        return std::nullopt;
    }

    if (it->second.is_expired(now)) {
        // Upgrade to unique lock to erase expired key
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return std::nullopt;
    }

    if (it->second.type != RedisType::STRING) {
        return std::nullopt;
    }

    // Touch LRU list under fast spinlock
    {
        SpinLockGuard lru_guard(lru_lock_);
        lru_.touch(key);
    }

    return it->second.as_string();
}

bool StorageShard::del_locked(const std::string& key) {
    auto it = data_.find(key);
    if (it == data_.end()) return false;

    size_t mem = it->second.estimate_memory() + key.capacity() + sizeof(void*);
    update_memory_delta(-static_cast<ssize_t>(mem));
    lru_.erase(key);
    ttl_manager_.remove_expiry(key);
    data_.erase(it);
    return true;
}

bool StorageShard::del(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    check_and_purge_expired_locked(key, now);
    return del_locked(key);
}

bool StorageShard::exists(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return false;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return false;
    }
    return true;
}

std::optional<RedisType> StorageShard::type(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return std::nullopt;
    }
    return it->second.type;
}

bool StorageShard::expire(const std::string& key, int64_t expire_at_ms) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    if (check_and_purge_expired_locked(key, now)) return false;

    auto it = data_.find(key);
    if (it == data_.end()) return false;

    it->second.expire_at_ms = expire_at_ms;
    ttl_manager_.set_expiry(key, expire_at_ms);
    return true;
}

int64_t StorageShard::ttl_ms(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return -2; // -2 = key does not exist

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return -2;
    }

    if (it->second.expire_at_ms < 0) return -1; // -1 = no expiration set
    int64_t rem = it->second.expire_at_ms - now;
    return rem > 0 ? rem : 0;
}

bool StorageShard::persist(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    if (check_and_purge_expired_locked(key, now)) return false;

    auto it = data_.find(key);
    if (it == data_.end() || it->second.expire_at_ms < 0) return false;

    it->second.expire_at_ms = -1;
    ttl_manager_.remove_expiry(key);
    return true;
}

int StorageShard::hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& field_values) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    check_and_purge_expired_locked(key, now);

    auto it = data_.find(key);
    if (it != data_.end() && it->second.type != RedisType::HASH) {
        return -1; // Type mismatch
    }

    size_t old_mem = 0;
    if (it == data_.end()) {
        data_[key] = ValueObject::make_hash();
        it = data_.find(key);
    } else {
        old_mem = it->second.estimate_memory();
    }

    auto& hash_map = it->second.as_hash();
    int added_count = 0;

    for (const auto& [f, v] : field_values) {
        auto [fit, inserted] = hash_map.insert_or_assign(f, v);
        if (inserted) ++added_count;
    }

    size_t new_mem = it->second.estimate_memory();
    ssize_t delta = static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem);
    update_memory_delta(delta);

    lru_.insert(key);
    return added_count;
}

std::optional<std::string> StorageShard::hget(const std::string& key, const std::string& field) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return std::nullopt;
    }

    if (it->second.type != RedisType::HASH) return std::nullopt;

    const auto& hash_map = it->second.as_hash();
    auto fit = hash_map.find(field);
    if (fit == hash_map.end()) return std::nullopt;

    {
        SpinLockGuard lru_guard(lru_lock_);
        lru_.touch(key);
    }
    return fit->second;
}

bool StorageShard::hdel(const std::string& key, const std::string& field) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    if (check_and_purge_expired_locked(key, now)) return false;

    auto it = data_.find(key);
    if (it == data_.end() || it->second.type != RedisType::HASH) return false;

    auto& hash_map = it->second.as_hash();
    size_t old_mem = it->second.estimate_memory();
    size_t erased = hash_map.erase(field);
    if (erased > 0) {
        if (hash_map.empty()) {
            del_locked(key);
        } else {
            size_t new_mem = it->second.estimate_memory();
            update_memory_delta(static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem));
        }
        return true;
    }
    return false;
}

std::optional<std::unordered_map<std::string, std::string>> StorageShard::hgetall(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return std::nullopt;
    }

    if (it->second.type != RedisType::HASH) return std::nullopt;

    {
        SpinLockGuard lru_guard(lru_lock_);
        lru_.touch(key);
    }
    return it->second.as_hash();
}

bool StorageShard::hexists(const std::string& key, const std::string& field) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return false;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return false;
    }

    if (it->second.type != RedisType::HASH) return false;
    return it->second.as_hash().find(field) != it->second.as_hash().end();
}

size_t StorageShard::hlen(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return 0;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return 0;
    }

    if (it->second.type != RedisType::HASH) return 0;
    return it->second.as_hash().size();
}

size_t StorageShard::lpush(const std::string& key, const std::vector<std::string>& values) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    check_and_purge_expired_locked(key, now);

    auto it = data_.find(key);
    if (it != data_.end() && it->second.type != RedisType::LIST) return 0;

    size_t old_mem = 0;
    if (it == data_.end()) {
        data_[key] = ValueObject::make_list();
        it = data_.find(key);
    } else {
        old_mem = it->second.estimate_memory();
    }

    auto& list = it->second.as_list();
    for (const auto& v : values) {
        list.push_front(v);
    }

    size_t new_mem = it->second.estimate_memory();
    update_memory_delta(static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem));
    lru_.insert(key);
    return list.size();
}

size_t StorageShard::rpush(const std::string& key, const std::vector<std::string>& values) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    check_and_purge_expired_locked(key, now);

    auto it = data_.find(key);
    if (it != data_.end() && it->second.type != RedisType::LIST) return 0;

    size_t old_mem = 0;
    if (it == data_.end()) {
        data_[key] = ValueObject::make_list();
        it = data_.find(key);
    } else {
        old_mem = it->second.estimate_memory();
    }

    auto& list = it->second.as_list();
    for (const auto& v : values) {
        list.push_back(v);
    }

    size_t new_mem = it->second.estimate_memory();
    update_memory_delta(static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem));
    lru_.insert(key);
    return list.size();
}

std::optional<std::string> StorageShard::lpop(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    if (check_and_purge_expired_locked(key, now)) return std::nullopt;

    auto it = data_.find(key);
    if (it == data_.end() || it->second.type != RedisType::LIST) return std::nullopt;

    auto& list = it->second.as_list();
    if (list.empty()) return std::nullopt;

    size_t old_mem = it->second.estimate_memory();
    std::string val = std::move(list.front());
    list.pop_front();

    if (list.empty()) {
        del_locked(key);
    } else {
        size_t new_mem = it->second.estimate_memory();
        update_memory_delta(static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem));
    }
    return val;
}

std::optional<std::string> StorageShard::rpop(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    if (check_and_purge_expired_locked(key, now)) return std::nullopt;

    auto it = data_.find(key);
    if (it == data_.end() || it->second.type != RedisType::LIST) return std::nullopt;

    auto& list = it->second.as_list();
    if (list.empty()) return std::nullopt;

    size_t old_mem = it->second.estimate_memory();
    std::string val = std::move(list.back());
    list.pop_back();

    if (list.empty()) {
        del_locked(key);
    } else {
        size_t new_mem = it->second.estimate_memory();
        update_memory_delta(static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem));
    }
    return val;
}

size_t StorageShard::llen(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return 0;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return 0;
    }

    if (it->second.type != RedisType::LIST) return 0;
    return it->second.as_list().size();
}

std::vector<std::string> StorageShard::lrange(const std::string& key, int64_t start, int64_t stop) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return {};

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return {};
    }

    if (it->second.type != RedisType::LIST) return {};

    const auto& list = it->second.as_list();
    int64_t size = static_cast<int64_t>(list.size());
    if (size == 0) return {};

    // Standard Redis negative index translation
    if (start < 0) start += size;
    if (stop < 0) stop += size;
    if (start < 0) start = 0;
    if (stop >= size) stop = size - 1;
    if (start > stop || start >= size) return {};

    std::vector<std::string> result;
    result.reserve(stop - start + 1);

    for (int64_t i = start; i <= stop; ++i) {
        result.push_back(list[i]);
    }
    return result;
}

size_t StorageShard::sadd(const std::string& key, const std::vector<std::string>& members) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    check_and_purge_expired_locked(key, now);

    auto it = data_.find(key);
    if (it != data_.end() && it->second.type != RedisType::SET) return 0;

    size_t old_mem = 0;
    if (it == data_.end()) {
        data_[key] = ValueObject::make_set();
        it = data_.find(key);
    } else {
        old_mem = it->second.estimate_memory();
    }

    auto& set = it->second.as_set();
    size_t added = 0;
    for (const auto& m : members) {
        if (set.insert(m).second) {
            ++added;
        }
    }

    size_t new_mem = it->second.estimate_memory();
    update_memory_delta(static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem));
    lru_.insert(key);
    return added;
}

size_t StorageShard::srem(const std::string& key, const std::vector<std::string>& members) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    if (check_and_purge_expired_locked(key, now)) return 0;

    auto it = data_.find(key);
    if (it == data_.end() || it->second.type != RedisType::SET) return 0;

    auto& set = it->second.as_set();
    size_t old_mem = it->second.estimate_memory();
    size_t removed = 0;

    for (const auto& m : members) {
        removed += set.erase(m);
    }

    if (set.empty()) {
        del_locked(key);
    } else {
        size_t new_mem = it->second.estimate_memory();
        update_memory_delta(static_cast<ssize_t>(new_mem) - static_cast<ssize_t>(old_mem));
    }
    return removed;
}

bool StorageShard::sismember(const std::string& key, const std::string& member) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return false;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return false;
    }

    if (it->second.type != RedisType::SET) return false;
    return it->second.as_set().find(member) != it->second.as_set().end();
}

std::vector<std::string> StorageShard::smembers(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return {};

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return {};
    }

    if (it->second.type != RedisType::SET) return {};

    const auto& set = it->second.as_set();
    std::vector<std::string> res;
    res.reserve(set.size());
    for (const auto& m : set) {
        res.push_back(m);
    }
    return res;
}

size_t StorageShard::scard(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) return 0;

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return 0;
    }

    if (it->second.type != RedisType::SET) return 0;
    return it->second.as_set().size();
}

bool StorageShard::evict_one(EvictionPolicy policy) {
    std::string key_to_evict;

    switch (policy) {
        case EvictionPolicy::ALLKEYS_LRU: {
            auto lru_key = lru_.pop_lru();
            if (lru_key.has_value()) {
                key_to_evict = std::move(lru_key.value());
            }
            break;
        }
        case EvictionPolicy::VOLATILE_LRU:
        case EvictionPolicy::VOLATILE_TTL: {
            // Find key with earliest expiration
            std::vector<std::string> expired;
            if (ttl_manager_.sweep_expired(INT64_MAX, 1, expired) > 0) {
                key_to_evict = expired[0];
            } else {
                auto lru_key = lru_.pop_lru();
                if (lru_key.has_value()) key_to_evict = std::move(lru_key.value());
            }
            break;
        }
        case EvictionPolicy::ALLKEYS_RANDOM: {
            if (!data_.empty()) {
                key_to_evict = data_.begin()->first;
            }
            break;
        }
        case EvictionPolicy::NO_EVICTION:
            return false;
    }

    if (key_to_evict.empty()) return false;

    auto it = data_.find(key_to_evict);
    if (it != data_.end()) {
        size_t mem = it->second.estimate_memory() + key_to_evict.capacity() + sizeof(void*);
        update_memory_delta(-static_cast<ssize_t>(mem));
        ttl_manager_.remove_expiry(key_to_evict);
        data_.erase(it);
        return true;
    }
    return false;
}

size_t StorageShard::sweep_expired(size_t max_keys) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    std::vector<std::string> expired;
    size_t count = ttl_manager_.sweep_expired(now, max_keys, expired);

    for (const auto& k : expired) {
        auto it = data_.find(k);
        if (it != data_.end()) {
            size_t mem = it->second.estimate_memory() + k.capacity() + sizeof(void*);
            update_memory_delta(-static_cast<ssize_t>(mem));
            lru_.erase(k);
            data_.erase(it);
        }
    }
    return count;
}

void StorageShard::sample_and_sweep(size_t sample_count, size_t& sampled, size_t& expired) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    std::vector<std::pair<std::string, int64_t>> samples;
    ttl_manager_.sample_keys(sample_count, samples);

    sampled = samples.size();
    expired = 0;

    for (const auto& [k, exp_time] : samples) {
        if (now >= exp_time) {
            auto it = data_.find(k);
            if (it != data_.end()) {
                size_t mem = it->second.estimate_memory() + k.capacity() + sizeof(void*);
                update_memory_delta(-static_cast<ssize_t>(mem));
                lru_.erase(k);
                ttl_manager_.remove_expiry(k);
                data_.erase(it);
                ++expired;
            }
        }
    }
}

void StorageShard::iterate_all(const std::function<void(const std::string& key, const ValueObject& val)>& callback) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    for (const auto& [k, v] : data_) {
        if (!v.is_expired(now)) {
            callback(k, v);
        }
    }
}

size_t StorageShard::key_count() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return data_.size();
}

size_t StorageShard::memory_usage() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return shard_memory_;
}

void StorageShard::flush() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    update_memory_delta(-static_cast<ssize_t>(shard_memory_));
    data_.clear();
    lru_.clear();
    ttl_manager_.clear();
}

} // namespace miniredis
