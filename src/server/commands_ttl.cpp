#include "server/command_registry.hpp"
#include "server/command_helpers.hpp"

namespace miniredis {

namespace {

// Shared implementation of EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT.
// unit_ms: 1000 for seconds, 1 for milliseconds. relative: add to now.
RespValue expire_generic(const Command& cmd, StorageEngine& engine, int64_t unit_ms, bool relative) {
    int64_t amount = 0;
    if (!parse_int64_strict(cmd.arg(1), amount)) {
        return not_integer_error();
    }

    int64_t deadline_ms = 0;
    if (!checked_mul_add(amount, unit_ms, relative ? now_ms() : 0, deadline_ms)) {
        return RespValue::make_error("ERR invalid expire time in '" + cmd.name() + "' command");
    }
    // Negative expire_at_ms means "no TTL" in storage, so a deadline in the
    // past (e.g. EXPIREAT k -5) used to make the key persistent instead of
    // expiring it.
    if (deadline_ms < 0) deadline_ms = 0;

    bool ok = engine.expire(cmd.arg(0), deadline_ms);
    return RespValue::make_integer(ok ? 1 : 0);
}

} // anonymous namespace

// EXPIRE key seconds
RespValue handle_expire(const Command& cmd, StorageEngine& engine, RedisServer*) {
    return expire_generic(cmd, engine, 1000, true);
}

// PEXPIRE key milliseconds
RespValue handle_pexpire(const Command& cmd, StorageEngine& engine, RedisServer*) {
    return expire_generic(cmd, engine, 1, true);
}

// EXPIREAT key unix-time-seconds
RespValue handle_expireat(const Command& cmd, StorageEngine& engine, RedisServer*) {
    return expire_generic(cmd, engine, 1000, false);
}

// PEXPIREAT key unix-time-milliseconds
RespValue handle_pexpireat(const Command& cmd, StorageEngine& engine, RedisServer*) {
    return expire_generic(cmd, engine, 1, false);
}

// TTL key
RespValue handle_ttl(const Command& cmd, StorageEngine& engine, RedisServer*) {
    int64_t rem_ms = engine.ttl_ms(cmd.arg(0));
    if (rem_ms < 0) {
        return RespValue::make_integer(rem_ms); // -2 (missing) or -1 (persistent)
    }
    return RespValue::make_integer((rem_ms + 500) / 1000);
}

// PTTL key
RespValue handle_pttl(const Command& cmd, StorageEngine& engine, RedisServer*) {
    return RespValue::make_integer(engine.ttl_ms(cmd.arg(0)));
}

// PERSIST key
RespValue handle_persist(const Command& cmd, StorageEngine& engine, RedisServer*) {
    bool ok = engine.persist(cmd.arg(0));
    return RespValue::make_integer(ok ? 1 : 0);
}

} // namespace miniredis
