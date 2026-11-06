#include "server/command_registry.hpp"
#include "server/command_helpers.hpp"
#include "server/redis_server.hpp"
#include <algorithm>
#include <cctype>

// Atomicity note: read-modify-write handlers (INCR, APPEND, SETNX, SET NX/XX...)
// read and then write in separate storage calls. They are atomic because
// RedisServer runs every write command while holding the lock stripes of all
// the keys it touches (see KeyLockTable), so no other writer can interleave.

namespace miniredis {

namespace {

std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
    return s;
}

RespValue invalid_expire(const char* cmd_name) {
    return RespValue::make_error(std::string("ERR invalid expire time in '") + cmd_name + "' command");
}

RespValue incr_by(const Command& cmd, StorageEngine& engine, int64_t delta) {
    const std::string& key = cmd.arg(0);
    std::string current_str;
    int64_t current = 0;

    switch (engine.get_string_checked(key, current_str)) {
        case LookupStatus::WRONG_TYPE:
            // Previously the hash/list/set was silently overwritten with a number.
            return wrongtype_error();
        case LookupStatus::FOUND:
            if (!parse_int64_strict(current_str, current)) {
                return not_integer_error();
            }
            break;
        case LookupStatus::MISSING:
            break;
    }

    int64_t next = 0;
    if (__builtin_add_overflow(current, delta, &next)) {
        return RespValue::make_error("ERR increment or decrement would overflow");
    }

    // keep_ttl: INCR must not make a key with a TTL persistent.
    if (!engine.set_string(key, std::to_string(next), -1, /*keep_ttl=*/true)) {
        return oom_error();
    }
    return RespValue::make_integer(next);
}

} // anonymous namespace

// GET key
RespValue handle_get(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    std::string val;
    switch (engine.get_string_checked(key, val)) {
        case LookupStatus::FOUND:
            if (server) server->stats().record_hit();
            return RespValue::make_bulk_string(std::move(val));
        case LookupStatus::WRONG_TYPE:
            return wrongtype_error();
        case LookupStatus::MISSING:
            break;
    }
    if (server) server->stats().record_miss();
    return RespValue::make_null_bulk();
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-s | PXAT unix-ms | KEEPTTL]
RespValue handle_set(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    const std::string& val = cmd.arg(1);
    const auto& tokens = cmd.tokens();

    bool nx = false;
    bool xx = false;
    bool get = false;
    bool keep_ttl = false;
    int expiry_options = 0;
    int64_t expire_at_ms = -1;

    // Options start after key and value (index 2 is the value, not an option)
    for (size_t i = 3; i < tokens.size(); ++i) {
        const std::string opt = to_upper(tokens[i]);
        const bool has_arg = i + 1 < tokens.size();

        if (opt == "NX") {
            nx = true;
        } else if (opt == "XX") {
            xx = true;
        } else if (opt == "GET") {
            get = true;
        } else if (opt == "KEEPTTL") {
            keep_ttl = true;
            ++expiry_options;
        } else if ((opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT") && has_arg) {
            int64_t n = 0;
            if (!parse_int64_strict(tokens[i + 1], n)) return not_integer_error();
            if (n <= 0) return invalid_expire("set");

            bool ok = true;
            if (opt == "EX")        ok = relative_ttl_to_deadline(n, 1000, expire_at_ms);
            else if (opt == "PX")   ok = relative_ttl_to_deadline(n, 1, expire_at_ms);
            else if (opt == "EXAT") ok = checked_mul_add(n, 1000, 0, expire_at_ms);
            else                    expire_at_ms = n;
            if (!ok) return invalid_expire("set");

            ++expiry_options;
            ++i;
        } else {
            // Unknown option, or EX/PX/EXAT/PXAT without a value
            return RespValue::make_error("ERR syntax error");
        }
    }

    if ((nx && xx) || expiry_options > 1) {
        return RespValue::make_error("ERR syntax error");
    }

    RespValue old_value = RespValue::make_null_bulk();
    if (get) {
        std::string old;
        LookupStatus st = engine.get_string_checked(key, old);
        if (st == LookupStatus::WRONG_TYPE) return wrongtype_error();
        if (st == LookupStatus::FOUND) old_value = RespValue::make_bulk_string(std::move(old));
    }

    bool exists = engine.exists(key);
    if ((nx && exists) || (xx && !exists)) {
        return get ? old_value : RespValue::make_null_bulk();
    }

    if (!engine.set_string(key, val, expire_at_ms, keep_ttl)) {
        return oom_error();
    }
    return get ? old_value : RespValue::make_ok();
}

// SETEX key seconds value
RespValue handle_setex(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t sec = 0;
    if (!parse_int64_strict(cmd.arg(1), sec)) return not_integer_error();

    int64_t exp = 0;
    if (sec <= 0 || !relative_ttl_to_deadline(sec, 1000, exp)) {
        return invalid_expire("setex");
    }

    if (!engine.set_string(key, cmd.arg(2), exp)) {
        return oom_error();
    }
    return RespValue::make_ok();
}

// SETNX key value
RespValue handle_setnx(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    const std::string& val = cmd.arg(1);

    if (engine.exists(key)) {
        return RespValue::make_integer(0);
    }
    if (!engine.set_string(key, val)) {
        return oom_error();
    }
    return RespValue::make_integer(1);
}

// DEL key [key ...]
RespValue handle_del(const Command& cmd, StorageEngine& engine, RedisServer*) {
    int64_t deleted_count = 0;
    for (size_t i = 0; i < cmd.arg_count(); ++i) {
        if (engine.del(cmd.arg(i))) {
            ++deleted_count;
        }
    }
    return RespValue::make_integer(deleted_count);
}

// EXISTS key [key ...]
RespValue handle_exists(const Command& cmd, StorageEngine& engine, RedisServer*) {
    int64_t exists_count = 0;
    for (size_t i = 0; i < cmd.arg_count(); ++i) {
        if (engine.exists(cmd.arg(i))) {
            ++exists_count;
        }
    }
    return RespValue::make_integer(exists_count);
}

// INCR key
RespValue handle_incr(const Command& cmd, StorageEngine& engine, RedisServer*) {
    return incr_by(cmd, engine, 1);
}

// DECR key
RespValue handle_decr(const Command& cmd, StorageEngine& engine, RedisServer*) {
    return incr_by(cmd, engine, -1);
}

// INCRBY key increment
RespValue handle_incrby(const Command& cmd, StorageEngine& engine, RedisServer*) {
    int64_t delta = 0;
    if (!parse_int64_strict(cmd.arg(1), delta)) return not_integer_error();
    return incr_by(cmd, engine, delta);
}

// DECRBY key decrement
RespValue handle_decrby(const Command& cmd, StorageEngine& engine, RedisServer*) {
    int64_t delta = 0;
    if (!parse_int64_strict(cmd.arg(1), delta)) return not_integer_error();
    if (delta == std::numeric_limits<int64_t>::min()) {
        return RespValue::make_error("ERR decrement would overflow");
    }
    return incr_by(cmd, engine, -delta);
}

// MGET key [key ...]
RespValue handle_mget(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    std::vector<std::string> keys;
    keys.reserve(cmd.arg_count());
    for (size_t i = 0; i < cmd.arg_count(); ++i) {
        keys.push_back(cmd.arg(i));
    }

    auto res = engine.mget(keys);
    RespArray arr;
    arr.reserve(res.size());

    for (const auto& item : res) {
        if (item.has_value()) {
            if (server) server->stats().record_hit();
            arr.push_back(RespValue::make_bulk_string(item.value()));
        } else {
            if (server) server->stats().record_miss();
            arr.push_back(RespValue::make_null_bulk());
        }
    }
    return RespValue::make_array(std::move(arr));
}

// MSET key value [key value ...]
RespValue handle_mset(const Command& cmd, StorageEngine& engine, RedisServer*) {
    if (cmd.arg_count() % 2 != 0) {
        return RespValue::make_error("ERR wrong number of arguments for 'mset' command");
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(cmd.arg_count() / 2);
    for (size_t i = 0; i < cmd.arg_count(); i += 2) {
        kvs.emplace_back(cmd.arg(i), cmd.arg(i + 1));
    }

    if (!engine.mset(kvs)) {
        return oom_error();
    }
    return RespValue::make_ok();
}

// APPEND key value
RespValue handle_append(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    const std::string& append_str = cmd.arg(1);

    std::string new_val;
    if (engine.get_string_checked(key, new_val) == LookupStatus::WRONG_TYPE) {
        return wrongtype_error();
    }
    new_val += append_str;

    if (!engine.set_string(key, new_val, -1, /*keep_ttl=*/true)) {
        return oom_error();
    }
    return RespValue::make_integer(static_cast<int64_t>(new_val.size()));
}

// STRLEN key
RespValue handle_strlen(const Command& cmd, StorageEngine& engine, RedisServer*) {
    std::string val;
    switch (engine.get_string_checked(cmd.arg(0), val)) {
        case LookupStatus::WRONG_TYPE: return wrongtype_error();
        case LookupStatus::FOUND:      return RespValue::make_integer(static_cast<int64_t>(val.size()));
        case LookupStatus::MISSING:    break;
    }
    return RespValue::make_integer(0);
}

} // namespace miniredis
