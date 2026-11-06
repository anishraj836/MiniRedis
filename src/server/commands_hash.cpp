#include "server/command_registry.hpp"
#include "server/command_helpers.hpp"
#include "server/redis_server.hpp"

namespace miniredis {

// HSET key field value [field value ...]
RespValue handle_hset(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if ((cmd.arg_count() - 1) % 2 != 0) {
        return RespValue::make_error("ERR wrong number of arguments for 'hset' command");
    }

    std::vector<std::pair<std::string, std::string>> field_values;
    field_values.reserve((cmd.arg_count() - 1) / 2);
    for (size_t i = 1; i < cmd.arg_count(); i += 2) {
        field_values.emplace_back(cmd.arg(i), cmd.arg(i + 1));
    }

    int64_t res = engine.get_shard(key).hset(key, field_values);
    if (res == StorageShard::WRONG_TYPE) return wrongtype_error();
    if (res == StorageShard::OUT_OF_MEMORY) return oom_error();
    return RespValue::make_integer(res);
}

// HGET key field
RespValue handle_hget(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::HASH)) return wrongtype_error();

    auto val = engine.get_shard(key).hget(key, cmd.arg(1));
    if (val.has_value()) {
        if (server) server->stats().record_hit();
        return RespValue::make_bulk_string(val.value());
    }
    if (server) server->stats().record_miss();
    return RespValue::make_null_bulk();
}

// HDEL key field [field ...]
RespValue handle_hdel(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::HASH)) return wrongtype_error();

    int64_t deleted = 0;
    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        if (engine.get_shard(key).hdel(key, cmd.arg(i))) {
            ++deleted;
        }
    }
    return RespValue::make_integer(deleted);
}

// HGETALL key
RespValue handle_hgetall(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::HASH)) return wrongtype_error();

    auto all_opt = engine.get_shard(key).hgetall(key);
    if (!all_opt.has_value()) {
        if (server) server->stats().record_miss();
        return RespValue::make_array({});
    }

    if (server) server->stats().record_hit();
    const auto& map = all_opt.value();
    RespArray arr;
    arr.reserve(map.size() * 2);

    for (const auto& [f, v] : map) {
        arr.push_back(RespValue::make_bulk_string(f));
        arr.push_back(RespValue::make_bulk_string(v));
    }
    return RespValue::make_array(std::move(arr));
}

// HEXISTS key field
RespValue handle_hexists(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::HASH)) return wrongtype_error();
    bool ex = engine.get_shard(key).hexists(key, cmd.arg(1));
    return RespValue::make_integer(ex ? 1 : 0);
}

// HLEN key
RespValue handle_hlen(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::HASH)) return wrongtype_error();
    size_t len = engine.get_shard(key).hlen(key);
    return RespValue::make_integer(static_cast<int64_t>(len));
}

// HKEYS key
RespValue handle_hkeys(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::HASH)) return wrongtype_error();

    auto all_opt = engine.get_shard(key).hgetall(key);
    if (!all_opt.has_value()) {
        return RespValue::make_array({});
    }

    RespArray arr;
    arr.reserve(all_opt->size());
    for (const auto& [f, v] : all_opt.value()) {
        arr.push_back(RespValue::make_bulk_string(f));
    }
    return RespValue::make_array(std::move(arr));
}

// HVALS key
RespValue handle_hvals(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::HASH)) return wrongtype_error();

    auto all_opt = engine.get_shard(key).hgetall(key);
    if (!all_opt.has_value()) {
        return RespValue::make_array({});
    }

    RespArray arr;
    arr.reserve(all_opt->size());
    for (const auto& [f, v] : all_opt.value()) {
        arr.push_back(RespValue::make_bulk_string(v));
    }
    return RespValue::make_array(std::move(arr));
}

// HMGET key field [field ...]
RespValue handle_hmget(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::HASH)) return wrongtype_error();

    RespArray arr;
    arr.reserve(cmd.arg_count() - 1);

    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        auto val = engine.get_shard(key).hget(key, cmd.arg(i));
        if (val.has_value()) {
            if (server) server->stats().record_hit();
            arr.push_back(RespValue::make_bulk_string(val.value()));
        } else {
            if (server) server->stats().record_miss();
            arr.push_back(RespValue::make_null_bulk());
        }
    }
    return RespValue::make_array(std::move(arr));
}

// HMSET key field value [field value ...]
RespValue handle_hmset(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    RespValue res = handle_hset(cmd, engine, server);
    if (res.type == RespType::ERROR) return res;
    return RespValue::make_ok(); // HMSET replies +OK, not the number of new fields
}

// HINCRBY key field increment
RespValue handle_hincrby(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    const std::string& field = cmd.arg(1);

    int64_t incr = 0;
    if (!parse_int64_strict(cmd.arg(2), incr)) {
        return not_integer_error();
    }
    if (holds_other_type(engine, key, RedisType::HASH)) {
        return wrongtype_error();
    }

    int64_t current = 0;
    auto existing = engine.get_shard(key).hget(key, field);
    if (existing.has_value() && !parse_int64_strict(existing.value(), current)) {
        return RespValue::make_error("ERR hash value is not an integer");
    }

    int64_t next = 0;
    if (__builtin_add_overflow(current, incr, &next)) {
        return RespValue::make_error("ERR increment or decrement would overflow");
    }

    int64_t res = engine.get_shard(key).hset(key, {{field, std::to_string(next)}});
    if (res == StorageShard::WRONG_TYPE) return wrongtype_error();
    if (res == StorageShard::OUT_OF_MEMORY) return oom_error();
    return RespValue::make_integer(next);
}

} // namespace miniredis
