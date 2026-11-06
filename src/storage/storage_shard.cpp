#include "storage/storage_shard.hpp"
#include <mutex>
#include <algorithm>

namespace miniredis {

namespace {

// Per-element costs. These must match ValueObject::estimate_memory() exactly:
// collection writes update the shard counter incrementally with them (O(1) per
// element) instead of re-estimating the whole collection on every write (O(n)).
size_t list_item_cost(const std::string& item) noexcept {
    return sizeof(void*) * 2 + sizeof(std::string) + item.capacity();
}

size_t hash_field_cost(const std::string& field, const std::string& value) noexcept {
    return sizeof(void*) * 3 + field.capacity() + value.capacity();
}

size_t set_member_cost(const std::string& member) noexcept {
    return sizeof(void*) * 3 + member.capacity();
}

ssize_t bucket_delta(size_t before, size_t after) noexcept {
    return (static_cast<ssize_t>(after) - static_cast<ssize_t>(before)) * static_cast<ssize_t>(sizeof(void*));
}

} // anonymous namespace

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

bool StorageShard::reserve_memory_locked(size_t bytes) {
    return try_evict_for_space(bytes);
}

void StorageShard::erase_locked(DataMap::iterator it) {
    update_memory_delta(-static_cast<ssize_t>(entry_cost(it->first, it->second)));
    lru_.erase(it->first);
    ttl_manager_.remove_expiry(it->first);
    data_.erase(it);
}

bool StorageShard::check_and_purge_expired_locked(const std::string& key, int64_t now) {
    auto it = data_.find(key);
    if (it == data_.end()) return false;

    if (it->second.is_expired(now)) {
        erase_locked(it);
        return true;
    }
    return false;
}

bool StorageShard::set_locked(const std::string& key, ValueObject value, bool keep_ttl, bool skip_admission) {
    int64_t now = now_ms();
    check_and_purge_expired_locked(key, now);

    auto it = data_.find(key);
    if (keep_ttl && it != data_.end()) {
        value.expire_at_ms = it->second.expire_at_ms;
    }

    if (!skip_admission) {
        size_t old_cost = (it != data_.end()) ? entry_cost(it->first, it->second) : 0;
        size_t new_cost = value.estimate_memory() + key.capacity() + sizeof(void*);
        if (new_cost > old_cost) {
            // Don't let the key we are about to overwrite be picked as the victim.
            if (it != data_.end()) lru_.touch(key);
            if (!try_evict_for_space(new_cost - old_cost)) {
                return false; // Out of memory and eviction failed / noeviction
            }
            it = data_.find(key); // eviction may have removed it
        }
    }

    if (value.expire_at_ms >= 0) {
        ttl_manager_.set_expiry(key, value.expire_at_ms);
    } else {
        ttl_manager_.remove_expiry(key);
    }

    lru_.insert(key);
    if (it != data_.end()) {
        size_t old_cost = entry_cost(it->first, it->second);
        it->second = std::move(value);
        update_memory_delta(static_cast<ssize_t>(entry_cost(it->first, it->second)) - static_cast<ssize_t>(old_cost));
    } else {
        it = data_.emplace(key, std::move(value)).first;
        update_memory_delta(static_cast<ssize_t>(entry_cost(it->first, it->second)));
    }
    return true;
}

bool StorageShard::set(const std::string& key, ValueObject value, bool keep_ttl) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return set_locked(key, std::move(value), keep_ttl, false);
}

bool StorageShard::set_string(const std::string& key, std::string val, int64_t expire_at_ms, bool keep_ttl) {
    return set(key, ValueObject::make_string(std::move(val), expire_at_ms), keep_ttl);
}

std::optional<std::string> StorageShard::get_string_shared_locked(const std::string& key, int64_t now) {
    auto it = data_.find(key);
    if (it == data_.end() || it->second.is_expired(now) || it->second.type != RedisType::STRING) {
        return std::nullopt;
    }
    {
        SpinLockGuard lru_guard(lru_lock_);
        lru_.touch(key);
    }
    return it->second.as_string();
}

LookupStatus StorageShard::get_string_checked(const std::string& key, std::string& out) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();

    auto it = data_.find(key);
    if (it == data_.end()) {
        return LookupStatus::MISSING;
    }

    if (it->second.is_expired(now)) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        check_and_purge_expired_locked(key, now);
        return LookupStatus::MISSING;
    }

    if (it->second.type != RedisType::STRING) {
        return LookupStatus::WRONG_TYPE;
    }

    {
        SpinLockGuard lru_guard(lru_lock_);
        lru_.touch(key);
    }
    out = it->second.as_string();
    return LookupStatus::FOUND;
}

std::optional<std::string> StorageShard::get_string(const std::string& key) {
    std::string out;
    if (get_string_checked(key, out) == LookupStatus::FOUND) {
        return out;
    }
    return std::nullopt;
}

bool StorageShard::del_locked(const std::string& key) {
    auto it = data_.find(key);
    if (it == data_.end()) return false;
    erase_locked(it);
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

StorageShard::DataMap::iterator StorageShard::prepare_collection_write_locked(
        const std::string& key, RedisType type, size_t incoming_bytes, int64_t& status) {
    int64_t now = now_ms();
    check_and_purge_expired_locked(key, now);

    auto it = data_.find(key);
    if (it != data_.end() && it->second.type != type) {
        status = WRONG_TYPE;
        return data_.end();
    }

    // maxmemory applies to collections too, not only to SET.
    if (it != data_.end()) lru_.touch(key);
    size_t needed = incoming_bytes + (it == data_.end() ? sizeof(ValueObject) + 48 + key.capacity() + sizeof(void*) : 0);
    if (!try_evict_for_space(needed)) {
        status = OUT_OF_MEMORY;
        return data_.end();
    }
    it = data_.find(key); // eviction may have removed it

    if (it == data_.end()) {
        ValueObject value;
        switch (type) {
            case RedisType::HASH: value = ValueObject::make_hash(); break;
            case RedisType::LIST: value = ValueObject::make_list(); break;
            case RedisType::SET:  value = ValueObject::make_set(); break;
            default:              value = ValueObject::make_string(""); break;
        }
        it = data_.emplace(key, std::move(value)).first;
        update_memory_delta(static_cast<ssize_t>(entry_cost(it->first, it->second)));
    }
    lru_.insert(key);
    status = 0;
    return it;
}

int64_t StorageShard::hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& field_values) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t incoming = 0;
    for (const auto& [f, v] : field_values) incoming += sizeof(void*) * 3 + f.size() + v.size();

    int64_t status = 0;
    auto it = prepare_collection_write_locked(key, RedisType::HASH, incoming, status);
    if (status < 0) return status;

    auto& hash_map = it->second.as_hash();
    size_t buckets_before = hash_map.bucket_count();
    ssize_t delta = 0;
    int64_t added_count = 0;

    for (const auto& [f, v] : field_values) {
        auto fit = hash_map.find(f);
        if (fit != hash_map.end()) {
            size_t old_cap = fit->second.capacity();
            fit->second = v;
            delta += static_cast<ssize_t>(fit->second.capacity()) - static_cast<ssize_t>(old_cap);
        } else {
            auto nit = hash_map.emplace(f, v).first;
            delta += static_cast<ssize_t>(hash_field_cost(nit->first, nit->second));
            ++added_count;
        }
    }

    delta += bucket_delta(buckets_before, hash_map.bucket_count());
    update_memory_delta(delta);
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
    auto fit = hash_map.find(field);
    if (fit == hash_map.end()) return false;

    // Account for the removed field before anything else, otherwise deleting
    // the last field would leak its bytes in the memory counter forever.
    size_t buckets_before = hash_map.bucket_count();
    ssize_t delta = -static_cast<ssize_t>(hash_field_cost(fit->first, fit->second));
    hash_map.erase(fit);
    delta += bucket_delta(buckets_before, hash_map.bucket_count());
    update_memory_delta(delta);

    if (hash_map.empty()) {
        erase_locked(it);
    }
    return true;
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

int64_t StorageShard::lpush(const std::string& key, const std::vector<std::string>& values) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t incoming = 0;
    for (const auto& v : values) incoming += sizeof(void*) * 2 + sizeof(std::string) + v.size();

    int64_t status = 0;
    auto it = prepare_collection_write_locked(key, RedisType::LIST, incoming, status);
    if (status < 0) return status;

    auto& list = it->second.as_list();
    ssize_t delta = 0;
    for (const auto& v : values) {
        list.push_front(v);
        delta += static_cast<ssize_t>(list_item_cost(list.front()));
    }
    update_memory_delta(delta);
    return static_cast<int64_t>(list.size());
}

int64_t StorageShard::rpush(const std::string& key, const std::vector<std::string>& values) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t incoming = 0;
    for (const auto& v : values) incoming += sizeof(void*) * 2 + sizeof(std::string) + v.size();

    int64_t status = 0;
    auto it = prepare_collection_write_locked(key, RedisType::LIST, incoming, status);
    if (status < 0) return status;

    auto& list = it->second.as_list();
    ssize_t delta = 0;
    for (const auto& v : values) {
        list.push_back(v);
        delta += static_cast<ssize_t>(list_item_cost(list.back()));
    }
    update_memory_delta(delta);
    return static_cast<int64_t>(list.size());
}

std::optional<std::string> StorageShard::lpop(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    if (check_and_purge_expired_locked(key, now)) return std::nullopt;

    auto it = data_.find(key);
    if (it == data_.end() || it->second.type != RedisType::LIST) return std::nullopt;

    auto& list = it->second.as_list();
    if (list.empty()) return std::nullopt;

    update_memory_delta(-static_cast<ssize_t>(list_item_cost(list.front())));
    std::string val = std::move(list.front());
    list.pop_front();

    if (list.empty()) {
        erase_locked(it);
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

    update_memory_delta(-static_cast<ssize_t>(list_item_cost(list.back())));
    std::string val = std::move(list.back());
    list.pop_back();

    if (list.empty()) {
        erase_locked(it);
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
    if (start < 0) start = (start < -size) ? 0 : start + size;
    if (stop < 0) stop = (stop < -size) ? -1 : stop + size;
    if (stop >= size) stop = size - 1;
    if (start > stop || start >= size) return {};

    std::vector<std::string> result;
    result.reserve(static_cast<size_t>(stop - start + 1));

    for (int64_t i = start; i <= stop; ++i) {
        result.push_back(list[static_cast<size_t>(i)]);
    }
    return result;
}

int64_t StorageShard::sadd(const std::string& key, const std::vector<std::string>& members) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t incoming = 0;
    for (const auto& m : members) incoming += sizeof(void*) * 3 + m.size();

    int64_t status = 0;
    auto it = prepare_collection_write_locked(key, RedisType::SET, incoming, status);
    if (status < 0) return status;

    auto& set = it->second.as_set();
    size_t buckets_before = set.bucket_count();
    ssize_t delta = 0;
    int64_t added = 0;
    for (const auto& m : members) {
        auto [mit, inserted] = set.insert(m);
        if (inserted) {
            delta += static_cast<ssize_t>(set_member_cost(*mit));
            ++added;
        }
    }

    delta += bucket_delta(buckets_before, set.bucket_count());
    update_memory_delta(delta);
    return added;
}

size_t StorageShard::srem(const std::string& key, const std::vector<std::string>& members) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    if (check_and_purge_expired_locked(key, now)) return 0;

    auto it = data_.find(key);
    if (it == data_.end() || it->second.type != RedisType::SET) return 0;

    auto& set = it->second.as_set();
    size_t buckets_before = set.bucket_count();
    ssize_t delta = 0;
    size_t removed = 0;

    for (const auto& m : members) {
        auto mit = set.find(m);
        if (mit != set.end()) {
            delta -= static_cast<ssize_t>(set_member_cost(*mit));
            set.erase(mit);
            ++removed;
        }
    }

    delta += bucket_delta(buckets_before, set.bucket_count());
    update_memory_delta(delta);

    if (set.empty()) {
        erase_locked(it);
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
    DataMap::iterator victim = data_.end();

    switch (policy) {
        case EvictionPolicy::ALLKEYS_LRU: {
            if (auto k = lru_.peek_lru()) {
                victim = data_.find(*k);
            }
            break;
        }
        case EvictionPolicy::VOLATILE_LRU: {
            // Least recently used key that has a TTL. Keys without a TTL are
            // never eligible under volatile-* policies.
            auto k = lru_.find_lru_if([this](const std::string& candidate) {
                auto it = data_.find(candidate);
                return it != data_.end() && it->second.expire_at_ms >= 0;
            }, 64);
            if (!k) k = ttl_manager_.soonest_key(); // fall back to any volatile key
            if (k) victim = data_.find(*k);
            break;
        }
        case EvictionPolicy::VOLATILE_TTL: {
            if (auto k = ttl_manager_.soonest_key()) {
                victim = data_.find(*k);
            }
            break;
        }
        case EvictionPolicy::ALLKEYS_RANDOM: {
            // Pick a random non-empty bucket: data_.begin() would always evict
            // the same few keys.
            size_t buckets = data_.bucket_count();
            for (int attempt = 0; attempt < 64 && victim == data_.end() && buckets > 0; ++attempt) {
                size_t b = rng_() % buckets;
                if (data_.bucket_size(b) > 0) {
                    victim = data_.find(data_.begin(b)->first);
                }
            }
            if (victim == data_.end() && !data_.empty()) victim = data_.begin();
            break;
        }
        case EvictionPolicy::NO_EVICTION:
            return false;
    }

    if (victim == data_.end()) return false;

    // erase_locked keeps data_, the LRU list and the TTL index in sync. The old
    // code skipped the LRU list for some policies, leaving stale keys there that
    // later stopped eviction early and caused false OOM errors.
    erase_locked(victim);
    if (mem_tracker_) mem_tracker_->record_eviction();
    return true;
}

size_t StorageShard::sweep_expired(size_t max_keys) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t now = now_ms();
    std::vector<std::string> expired;
    ttl_manager_.sweep_expired(now, max_keys, expired);

    size_t removed = 0;
    for (const auto& k : expired) {
        auto it = data_.find(k);
        if (it != data_.end() && it->second.is_expired(now)) {
            erase_locked(it);
            ++removed;
        }
    }
    return removed;
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

size_t StorageShard::expires_count() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return ttl_manager_.size();
}

size_t StorageShard::memory_usage() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return shard_memory_;
}

bool StorageShard::verify_memory_accounting() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    size_t recomputed = 0;
    for (const auto& [k, v] : data_) {
        recomputed += entry_cost(k, v);
    }
    return recomputed == shard_memory_ && lru_.size() == data_.size();
}

void StorageShard::flush() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    update_memory_delta(-static_cast<ssize_t>(shard_memory_));
    data_.clear();
    lru_.clear();
    ttl_manager_.clear();
}

} // namespace miniredis
