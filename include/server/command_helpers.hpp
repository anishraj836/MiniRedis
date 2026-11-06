#pragma once

#include "protocol/resp_types.hpp"
#include "storage/storage_engine.hpp"
#include <charconv>
#include <cstdint>
#include <limits>
#include <string>

namespace miniredis {

inline RespValue wrongtype_error() {
    return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
}

inline RespValue oom_error() {
    return RespValue::make_error("OOM command not allowed when used memory > maxmemory");
}

inline RespValue not_integer_error() {
    return RespValue::make_error("ERR value is not an integer or out of range");
}

// Parses the whole string as a base-10 int64. Rejects empty input, trailing
// garbage ("12abc") and out-of-range values.
inline bool parse_int64_strict(const std::string& s, int64_t& out) {
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
}

// True when `key` exists and holds a type other than `expected`.
inline bool holds_other_type(StorageEngine& engine, const std::string& key, RedisType expected) {
    auto t = engine.type(key);
    return t.has_value() && *t != expected;
}

// a * b + c without signed overflow (which is undefined behaviour in C++).
inline bool checked_mul_add(int64_t a, int64_t b, int64_t c, int64_t& out) {
    int64_t product = 0;
    if (__builtin_mul_overflow(a, b, &product)) return false;
    return !__builtin_add_overflow(product, c, &out);
}

// Absolute unix-ms deadline for a TTL of `amount` units of `unit_ms` from now.
inline bool relative_ttl_to_deadline(int64_t amount, int64_t unit_ms, int64_t& deadline_ms) {
    return checked_mul_add(amount, unit_ms, now_ms(), deadline_ms);
}

} // namespace miniredis
