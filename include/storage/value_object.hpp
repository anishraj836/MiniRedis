#pragma once

#include "common/types.hpp"
#include <string>
#include <unordered_map>
#include <deque>
#include <unordered_set>
#include <variant>
#include <cstdint>

namespace miniredis {

enum class RedisType : uint8_t {
    STRING,
    HASH,
    LIST,
    SET,
    NONE
};

inline const char* redis_type_to_string(RedisType type) noexcept {
    switch (type) {
        case RedisType::STRING: return "string";
        case RedisType::HASH:   return "hash";
        case RedisType::LIST:   return "list";
        case RedisType::SET:    return "set";
        case RedisType::NONE:   return "none";
    }
    return "none";
}

struct ValueObject {
    RedisType type{RedisType::NONE};
    int64_t expire_at_ms{-1}; // -1 = persistent / no expiry

    std::variant<
        std::monostate,
        std::string,                                  // STRING
        std::unordered_map<std::string, std::string>, // HASH
        std::deque<std::string>,                      // LIST
        std::unordered_set<std::string>               // SET
    > data;

    // Factory methods
    static ValueObject make_string(std::string str, int64_t expire_at = -1) {
        ValueObject obj;
        obj.type = RedisType::STRING;
        obj.expire_at_ms = expire_at;
        obj.data = std::move(str);
        return obj;
    }

    static ValueObject make_hash(int64_t expire_at = -1) {
        ValueObject obj;
        obj.type = RedisType::HASH;
        obj.expire_at_ms = expire_at;
        obj.data = std::unordered_map<std::string, std::string>{};
        return obj;
    }

    static ValueObject make_list(int64_t expire_at = -1) {
        ValueObject obj;
        obj.type = RedisType::LIST;
        obj.expire_at_ms = expire_at;
        obj.data = std::deque<std::string>{};
        return obj;
    }

    static ValueObject make_set(int64_t expire_at = -1) {
        ValueObject obj;
        obj.type = RedisType::SET;
        obj.expire_at_ms = expire_at;
        obj.data = std::unordered_set<std::string>{};
        return obj;
    }

    [[nodiscard]] bool is_expired(int64_t current_ms) const noexcept {
        return expire_at_ms >= 0 && current_ms >= expire_at_ms;
    }

    [[nodiscard]] int64_t ttl_ms(int64_t current_ms) const noexcept {
        if (expire_at_ms < 0) return -1;
        int64_t rem = expire_at_ms - current_ms;
        return rem > 0 ? rem : 0;
    }

    [[nodiscard]] int64_t ttl_seconds(int64_t current_ms) const noexcept {
        int64_t ms = ttl_ms(current_ms);
        if (ms < 0) return -1;
        return (ms + 500) / 1000; // Round to nearest second
    }

    [[nodiscard]] std::string& as_string() {
        return std::get<std::string>(data);
    }

    [[nodiscard]] const std::string& as_string() const {
        return std::get<std::string>(data);
    }

    [[nodiscard]] std::unordered_map<std::string, std::string>& as_hash() {
        return std::get<std::unordered_map<std::string, std::string>>(data);
    }

    [[nodiscard]] const std::unordered_map<std::string, std::string>& as_hash() const {
        return std::get<std::unordered_map<std::string, std::string>>(data);
    }

    [[nodiscard]] std::deque<std::string>& as_list() {
        return std::get<std::deque<std::string>>(data);
    }

    [[nodiscard]] const std::deque<std::string>& as_list() const {
        return std::get<std::deque<std::string>>(data);
    }

    [[nodiscard]] std::unordered_set<std::string>& as_set() {
        return std::get<std::unordered_set<std::string>>(data);
    }

    [[nodiscard]] const std::unordered_set<std::string>& as_set() const {
        return std::get<std::unordered_set<std::string>>(data);
    }

    // Dynamic memory footprint estimation in bytes
    [[nodiscard]] size_t estimate_memory() const noexcept {
        size_t base = sizeof(ValueObject) + 48; // Base struct + map node overhead
        switch (type) {
            case RedisType::STRING: {
                if (std::holds_alternative<std::string>(data)) {
                    base += std::get<std::string>(data).capacity();
                }
                break;
            }
            case RedisType::HASH: {
                if (std::holds_alternative<std::unordered_map<std::string, std::string>>(data)) {
                    const auto& h = std::get<std::unordered_map<std::string, std::string>>(data);
                    base += h.bucket_count() * sizeof(void*);
                    for (const auto& [k, v] : h) {
                        base += sizeof(void*) * 3 + k.capacity() + v.capacity();
                    }
                }
                break;
            }
            case RedisType::LIST: {
                if (std::holds_alternative<std::deque<std::string>>(data)) {
                    const auto& l = std::get<std::deque<std::string>>(data);
                    for (const auto& item : l) {
                        base += sizeof(void*) * 2 + sizeof(std::string) + item.capacity();
                    }
                }
                break;
            }
            case RedisType::SET: {
                if (std::holds_alternative<std::unordered_set<std::string>>(data)) {
                    const auto& s = std::get<std::unordered_set<std::string>>(data);
                    base += s.bucket_count() * sizeof(void*);
                    for (const auto& item : s) {
                        base += sizeof(void*) * 3 + item.capacity();
                    }
                }
                break;
            }
            case RedisType::NONE:
                break;
        }
        return base;
    }
};

} // namespace miniredis
