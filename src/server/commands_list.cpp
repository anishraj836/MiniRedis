#include "server/command_registry.hpp"
#include "server/redis_server.hpp"
#include <charconv>

namespace miniredis {

// LPUSH key element [element ...]
RespValue handle_lpush(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    std::vector<std::string> values;
    values.reserve(cmd.arg_count() - 1);

    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        values.push_back(cmd.arg(i));
    }

    size_t size = engine.get_shard(key).lpush(key, values);
    return RespValue::make_integer(static_cast<int64_t>(size));
}

// RPUSH key element [element ...]
RespValue handle_rpush(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    std::vector<std::string> values;
    values.reserve(cmd.arg_count() - 1);

    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        values.push_back(cmd.arg(i));
    }

    size_t size = engine.get_shard(key).rpush(key, values);
    return RespValue::make_integer(static_cast<int64_t>(size));
}

// LPOP key
RespValue handle_lpop(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    auto val = engine.get_shard(key).lpop(key);
    if (val.has_value()) {
        if (server) server->stats().record_hit();
        return RespValue::make_bulk_string(val.value());
    } else {
        if (server) server->stats().record_miss();
        return RespValue::make_null_bulk();
    }
}

// RPOP key
RespValue handle_rpop(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    auto val = engine.get_shard(key).rpop(key);
    if (val.has_value()) {
        if (server) server->stats().record_hit();
        return RespValue::make_bulk_string(val.value());
    } else {
        if (server) server->stats().record_miss();
        return RespValue::make_null_bulk();
    }
}

// LLEN key
RespValue handle_llen(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    size_t len = engine.get_shard(key).llen(key);
    return RespValue::make_integer(static_cast<int64_t>(len));
}

// LRANGE key start stop
RespValue handle_lrange(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t start = 0;
    int64_t stop = 0;

    auto [p1, ec1] = std::from_chars(cmd.arg(1).data(), cmd.arg(1).data() + cmd.arg(1).size(), start);
    auto [p2, ec2] = std::from_chars(cmd.arg(2).data(), cmd.arg(2).data() + cmd.arg(2).size(), stop);

    if (ec1 != std::errc() || ec2 != std::errc()) {
        return RespValue::make_error("ERR value is not an integer or out of range");
    }

    auto items = engine.get_shard(key).lrange(key, start, stop);
    RespArray arr;
    arr.reserve(items.size());

    for (const auto& item : items) {
        arr.push_back(RespValue::make_bulk_string(item));
    }
    return RespValue::make_array(std::move(arr));
}

} // namespace miniredis
