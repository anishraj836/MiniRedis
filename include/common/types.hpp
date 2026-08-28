#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <chrono>
#include <functional>
#include <optional>
#include <memory>
#include <variant>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <cstring>

namespace miniredis {

// Server and network defaults
constexpr uint16_t DEFAULT_PORT = 6379;
constexpr const char* DEFAULT_HOST = "127.0.0.1";
constexpr size_t DEFAULT_SHARD_COUNT = 64;
constexpr size_t DEFAULT_MAX_MEMORY = 256 * 1024 * 1024; // 256 MB
constexpr size_t BUFFER_INITIAL_CAPACITY = 16 * 1024;     // 16 KB

// High-resolution time utilities
inline int64_t now_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

inline int64_t now_us() noexcept {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

inline int64_t steady_now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
}

// Memory eviction policies
enum class EvictionPolicy {
    NO_EVICTION,
    ALLKEYS_LRU,
    VOLATILE_LRU,
    ALLKEYS_RANDOM,
    VOLATILE_TTL
};

inline std::string to_string(EvictionPolicy policy) {
    switch (policy) {
        case EvictionPolicy::NO_EVICTION:    return "noeviction";
        case EvictionPolicy::ALLKEYS_LRU:    return "allkeys-lru";
        case EvictionPolicy::VOLATILE_LRU:   return "volatile-lru";
        case EvictionPolicy::ALLKEYS_RANDOM: return "allkeys-random";
        case EvictionPolicy::VOLATILE_TTL:   return "volatile-ttl";
    }
    return "noeviction";
}

inline EvictionPolicy parse_eviction_policy(std::string_view str) {
    if (str == "allkeys-lru")    return EvictionPolicy::ALLKEYS_LRU;
    if (str == "volatile-lru")   return EvictionPolicy::VOLATILE_LRU;
    if (str == "allkeys-random") return EvictionPolicy::ALLKEYS_RANDOM;
    if (str == "volatile-ttl")   return EvictionPolicy::VOLATILE_TTL;
    return EvictionPolicy::NO_EVICTION;
}

// AOF Fsync policies
enum class AofFsyncPolicy {
    ALWAYS,
    EVERYSEC,
    NO
};

inline std::string to_string(AofFsyncPolicy policy) {
    switch (policy) {
        case AofFsyncPolicy::ALWAYS:   return "always";
        case AofFsyncPolicy::EVERYSEC: return "everysec";
        case AofFsyncPolicy::NO:       return "no";
    }
    return "everysec";
}

inline AofFsyncPolicy parse_aof_policy(std::string_view str) {
    if (str == "always") return AofFsyncPolicy::ALWAYS;
    if (str == "no")     return AofFsyncPolicy::NO;
    return AofFsyncPolicy::EVERYSEC;
}

// MurmurHash3 (32-bit) for ultra-fast and uniform key distribution across shards
inline uint32_t murmur3_32(const void* key, size_t len, uint32_t seed = 0x9747b28c) noexcept {
    const auto* data = static_cast<const uint8_t*>(key);
    const size_t nblocks = len / 4;
    uint32_t h1 = seed;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    // Body
    for (size_t i = 0; i < nblocks; ++i) {
        uint32_t k1;
        std::memcpy(&k1, data + i * 4, sizeof(uint32_t));

        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> 19);
        h1 = h1 * 5 + 0xe6546b64;
    }

    // Tail
    const auto* tail = data + nblocks * 4;
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= static_cast<uint32_t>(tail[2]) << 16; [[fallthrough]];
        case 2: k1 ^= static_cast<uint32_t>(tail[1]) << 8;  [[fallthrough]];
        case 1: k1 ^= static_cast<uint32_t>(tail[0]);
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >> 17);
                k1 *= c2;
                h1 ^= k1;
    }

    // Finalization
    h1 ^= static_cast<uint32_t>(len);
    h1 ^= (h1 >> 16);
    h1 *= 0x85ebca6b;
    h1 ^= (h1 >> 13);
    h1 *= 0xc2b2ae35;
    h1 ^= (h1 >> 16);

    return h1;
}

inline uint32_t hash_key(std::string_view key) noexcept {
    return murmur3_32(key.data(), key.size());
}

} // namespace miniredis
