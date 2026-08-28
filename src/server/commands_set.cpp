#include "server/command_registry.hpp"
#include "server/redis_server.hpp"

namespace miniredis {

// SADD key member [member ...]
RespValue handle_sadd(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    std::vector<std::string> members;
    members.reserve(cmd.arg_count() - 1);

    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        members.push_back(cmd.arg(i));
    }

    size_t added = engine.get_shard(key).sadd(key, members);
    return RespValue::make_integer(static_cast<int64_t>(added));
}

// SREM key member [member ...]
RespValue handle_srem(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    std::vector<std::string> members;
    members.reserve(cmd.arg_count() - 1);

    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        members.push_back(cmd.arg(i));
    }

    size_t removed = engine.get_shard(key).srem(key, members);
    return RespValue::make_integer(static_cast<int64_t>(removed));
}

// SISMEMBER key member
RespValue handle_sismember(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    const std::string& member = cmd.arg(1);
    bool is_member = engine.get_shard(key).sismember(key, member);
    return RespValue::make_integer(is_member ? 1 : 0);
}

// SMEMBERS key
RespValue handle_smembers(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    auto members = engine.get_shard(key).smembers(key);

    if (members.empty()) {
        if (server) server->stats().record_miss();
    } else {
        if (server) server->stats().record_hit();
    }

    RespArray arr;
    arr.reserve(members.size());
    for (const auto& m : members) {
        arr.push_back(RespValue::make_bulk_string(m));
    }
    return RespValue::make_array(std::move(arr));
}

// SCARD key
RespValue handle_scard(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    size_t count = engine.get_shard(key).scard(key);
    return RespValue::make_integer(static_cast<int64_t>(count));
}

// SUNION key [key ...]
RespValue handle_sunion(const Command& cmd, StorageEngine& engine, RedisServer*) {
    std::unordered_set<std::string> union_set;
    for (size_t i = 0; i < cmd.arg_count(); ++i) {
        auto members = engine.get_shard(cmd.arg(i)).smembers(cmd.arg(i));
        for (auto& m : members) {
            union_set.insert(std::move(m));
        }
    }

    RespArray arr;
    arr.reserve(union_set.size());
    for (const auto& m : union_set) {
        arr.push_back(RespValue::make_bulk_string(m));
    }
    return RespValue::make_array(std::move(arr));
}

// SINTER key [key ...]
RespValue handle_sinter(const Command& cmd, StorageEngine& engine, RedisServer*) {
    if (cmd.arg_count() == 0) {
        return RespValue::make_array({});
    }

    auto first_members = engine.get_shard(cmd.arg(0)).smembers(cmd.arg(0));
    std::unordered_set<std::string> current_set(first_members.begin(), first_members.end());

    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        if (current_set.empty()) break;
        auto members = engine.get_shard(cmd.arg(i)).smembers(cmd.arg(i));
        std::unordered_set<std::string> other_set(members.begin(), members.end());

        for (auto it = current_set.begin(); it != current_set.end(); ) {
            if (other_set.find(*it) == other_set.end()) {
                it = current_set.erase(it);
            } else {
                ++it;
            }
        }
    }

    RespArray arr;
    arr.reserve(current_set.size());
    for (const auto& m : current_set) {
        arr.push_back(RespValue::make_bulk_string(m));
    }
    return RespValue::make_array(std::move(arr));
}

// SDIFF key [key ...]
RespValue handle_sdiff(const Command& cmd, StorageEngine& engine, RedisServer*) {
    if (cmd.arg_count() == 0) {
        return RespValue::make_array({});
    }

    auto first_members = engine.get_shard(cmd.arg(0)).smembers(cmd.arg(0));
    std::unordered_set<std::string> diff_set(first_members.begin(), first_members.end());

    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        if (diff_set.empty()) break;
        auto members = engine.get_shard(cmd.arg(i)).smembers(cmd.arg(i));
        for (const auto& m : members) {
            diff_set.erase(m);
        }
    }

    RespArray arr;
    arr.reserve(diff_set.size());
    for (const auto& m : diff_set) {
        arr.push_back(RespValue::make_bulk_string(m));
    }
    return RespValue::make_array(std::move(arr));
}

} // namespace miniredis
