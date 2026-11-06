#include "server/command_registry.hpp"
#include "server/command_helpers.hpp"
#include "server/redis_server.hpp"

namespace miniredis {

namespace {

RespValue push_reply(int64_t res) {
    if (res == StorageShard::WRONG_TYPE) return wrongtype_error();
    if (res == StorageShard::OUT_OF_MEMORY) return oom_error();
    return RespValue::make_integer(res);
}

std::vector<std::string> collect_values(const Command& cmd) {
    std::vector<std::string> values;
    values.reserve(cmd.arg_count() - 1);
    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        values.push_back(cmd.arg(i));
    }
    return values;
}

RespValue pop_reply(std::optional<std::string> val, RedisServer* server) {
    if (val.has_value()) {
        if (server) server->stats().record_hit();
        return RespValue::make_bulk_string(std::move(*val));
    }
    if (server) server->stats().record_miss();
    return RespValue::make_null_bulk();
}

} // anonymous namespace

// LPUSH key element [element ...]
RespValue handle_lpush(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    return push_reply(engine.get_shard(key).lpush(key, collect_values(cmd)));
}

// RPUSH key element [element ...]
RespValue handle_rpush(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    return push_reply(engine.get_shard(key).rpush(key, collect_values(cmd)));
}

// LPOP key
RespValue handle_lpop(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::LIST)) return wrongtype_error();
    return pop_reply(engine.get_shard(key).lpop(key), server);
}

// RPOP key
RespValue handle_rpop(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::LIST)) return wrongtype_error();
    return pop_reply(engine.get_shard(key).rpop(key), server);
}

// LLEN key
RespValue handle_llen(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::LIST)) return wrongtype_error();
    size_t len = engine.get_shard(key).llen(key);
    return RespValue::make_integer(static_cast<int64_t>(len));
}

// LRANGE key start stop
RespValue handle_lrange(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t start = 0;
    int64_t stop = 0;

    if (!parse_int64_strict(cmd.arg(1), start) || !parse_int64_strict(cmd.arg(2), stop)) {
        return not_integer_error();
    }
    if (holds_other_type(engine, key, RedisType::LIST)) return wrongtype_error();

    auto items = engine.get_shard(key).lrange(key, start, stop);
    RespArray arr;
    arr.reserve(items.size());

    for (auto& item : items) {
        arr.push_back(RespValue::make_bulk_string(std::move(item)));
    }
    return RespValue::make_array(std::move(arr));
}

} // namespace miniredis
