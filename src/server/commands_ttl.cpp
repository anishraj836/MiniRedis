#include "server/command_registry.hpp"
#include <charconv>

namespace miniredis {

// EXPIRE key seconds
RespValue handle_expire(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t sec = 0;
    auto [p, ec] = std::from_chars(cmd.arg(1).data(), cmd.arg(1).data() + cmd.arg(1).size(), sec);
    if (ec != std::errc()) {
        return RespValue::make_error("ERR value is not an integer or out of range");
    }

    int64_t exp_ms = now_ms() + sec * 1000;
    bool ok = engine.expire(key, exp_ms);
    return RespValue::make_integer(ok ? 1 : 0);
}

// PEXPIRE key milliseconds
RespValue handle_pexpire(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t ms = 0;
    auto [p, ec] = std::from_chars(cmd.arg(1).data(), cmd.arg(1).data() + cmd.arg(1).size(), ms);
    if (ec != std::errc()) {
        return RespValue::make_error("ERR value is not an integer or out of range");
    }

    int64_t exp_ms = now_ms() + ms;
    bool ok = engine.expire(key, exp_ms);
    return RespValue::make_integer(ok ? 1 : 0);
}

// TTL key
RespValue handle_ttl(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t rem_ms = engine.ttl_ms(key);
    if (rem_ms < 0) {
        return RespValue::make_integer(rem_ms); // -2 (missing) or -1 (persistent)
    }
    return RespValue::make_integer((rem_ms + 500) / 1000);
}

// PTTL key
RespValue handle_pttl(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t rem_ms = engine.ttl_ms(key);
    return RespValue::make_integer(rem_ms);
}

// PERSIST key
RespValue handle_persist(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    bool ok = engine.persist(key);
    return RespValue::make_integer(ok ? 1 : 0);
}

// EXPIREAT key unix-time-seconds
RespValue handle_expireat(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t sec = 0;
    auto [p, ec] = std::from_chars(cmd.arg(1).data(), cmd.arg(1).data() + cmd.arg(1).size(), sec);
    if (ec != std::errc()) {
        return RespValue::make_error("ERR value is not an integer or out of range");
    }

    bool ok = engine.expire(key, sec * 1000);
    return RespValue::make_integer(ok ? 1 : 0);
}

// PEXPIREAT key unix-time-milliseconds
RespValue handle_pexpireat(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t ms = 0;
    auto [p, ec] = std::from_chars(cmd.arg(1).data(), cmd.arg(1).data() + cmd.arg(1).size(), ms);
    if (ec != std::errc()) {
        return RespValue::make_error("ERR value is not an integer or out of range");
    }

    bool ok = engine.expire(key, ms);
    return RespValue::make_integer(ok ? 1 : 0);
}

} // namespace miniredis
