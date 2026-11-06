#include "server/command_registry.hpp"
#include "server/redis_server.hpp"
#include <charconv>

namespace miniredis {

// GET key
RespValue handle_get(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    auto val = engine.get_string(key);
    if (val.has_value()) {
        if (server) server->stats().record_hit();
        return RespValue::make_bulk_string(val.value());
    } else {
        if (server) server->stats().record_miss();
        return RespValue::make_null_bulk();
    }
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds]
RespValue handle_set(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    const std::string& val = cmd.arg(1);

    bool nx = false;
    bool xx = false;
    int64_t expire_at_ms = -1;

    // Options start after key and value (index 2 is the value, not an option)
    for (size_t i = 3; i < cmd.tokens().size(); ++i) {
        std::string opt = cmd.tokens()[i];
        std::transform(opt.begin(), opt.end(), opt.begin(), [](unsigned char c) { return std::toupper(c); });

        if (opt == "NX") {
            nx = true;
        } else if (opt == "XX") {
            xx = true;
        } else if (opt == "EX" && i + 1 < cmd.tokens().size()) {
            int64_t sec = 0;
            auto [p, ec] = std::from_chars(cmd.tokens()[i + 1].data(), cmd.tokens()[i + 1].data() + cmd.tokens()[i + 1].size(), sec);
            if (ec != std::errc() || sec <= 0) return RespValue::make_error("ERR invalid expire time in set");
            expire_at_ms = now_ms() + sec * 1000;
            ++i;
        } else if (opt == "PX" && i + 1 < cmd.tokens().size()) {
            int64_t ms = 0;
            auto [p, ec] = std::from_chars(cmd.tokens()[i + 1].data(), cmd.tokens()[i + 1].data() + cmd.tokens()[i + 1].size(), ms);
            if (ec != std::errc() || ms <= 0) return RespValue::make_error("ERR invalid expire time in set");
            expire_at_ms = now_ms() + ms;
            ++i;
        } else {
            // Unknown option, or EX/PX without a value
            return RespValue::make_error("ERR syntax error");
        }
    }

    if (nx && xx) {
        return RespValue::make_error("ERR syntax error");
    }

    bool exists = engine.exists(key);
    if (nx && exists) {
        return RespValue::make_null_bulk();
    }
    if (xx && !exists) {
        return RespValue::make_null_bulk();
    }

    bool ok = engine.set_string(key, val, expire_at_ms);
    if (!ok) {
        return RespValue::make_error("OOM command not allowed when used memory > maxmemory");
    }
    return RespValue::make_ok();
}

// SETEX key seconds value
RespValue handle_setex(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t sec = 0;
    auto [p, ec] = std::from_chars(cmd.arg(1).data(), cmd.arg(1).data() + cmd.arg(1).size(), sec);
    if (ec != std::errc() || sec <= 0) {
        return RespValue::make_error("ERR invalid expire time in 'setex' command");
    }
    const std::string& val = cmd.arg(2);
    int64_t exp = now_ms() + sec * 1000;

    if (!engine.set_string(key, val, exp)) {
        return RespValue::make_error("OOM command not allowed when used memory > maxmemory");
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
        return RespValue::make_error("OOM command not allowed when used memory > maxmemory");
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
    const std::string& key = cmd.arg(0);
    auto val_opt = engine.get_string(key);
    int64_t current = 0;

    if (val_opt.has_value()) {
        const auto& s = val_opt.value();
        auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), current);
        if (ec != std::errc() || p != s.data() + s.size()) {
            return RespValue::make_error("ERR value is not an integer or out of range");
        }
    }

    ++current;
    engine.set_string(key, std::to_string(current));
    return RespValue::make_integer(current);
}

// DECR key
RespValue handle_decr(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    auto val_opt = engine.get_string(key);
    int64_t current = 0;

    if (val_opt.has_value()) {
        const auto& s = val_opt.value();
        auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), current);
        if (ec != std::errc() || p != s.data() + s.size()) {
            return RespValue::make_error("ERR value is not an integer or out of range");
        }
    }

    --current;
    engine.set_string(key, std::to_string(current));
    return RespValue::make_integer(current);
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
        return RespValue::make_error("OOM command not allowed when used memory > maxmemory");
    }
    return RespValue::make_ok();
}

// APPEND key value
RespValue handle_append(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    const std::string& append_str = cmd.arg(1);

    auto val_opt = engine.get_string(key);
    std::string new_val;
    if (val_opt.has_value()) {
        new_val = val_opt.value() + append_str;
    } else {
        new_val = append_str;
    }

    engine.set_string(key, new_val);
    return RespValue::make_integer(static_cast<int64_t>(new_val.size()));
}

// STRLEN key
RespValue handle_strlen(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    auto val_opt = engine.get_string(key);
    if (!val_opt.has_value()) {
        return RespValue::make_integer(0);
    }
    return RespValue::make_integer(static_cast<int64_t>(val_opt.value().size()));
}

} // namespace miniredis
