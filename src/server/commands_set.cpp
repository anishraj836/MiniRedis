#include "server/command_registry.hpp"
#include "server/command_helpers.hpp"
#include "server/redis_server.hpp"

namespace miniredis {

namespace {

std::vector<std::string> collect_members(const Command& cmd) {
    std::vector<std::string> members;
    members.reserve(cmd.arg_count() - 1);
    for (size_t i = 1; i < cmd.arg_count(); ++i) {
        members.push_back(cmd.arg(i));
    }
    return members;
}

RespValue to_array(const std::unordered_set<std::string>& set) {
    RespArray arr;
    arr.reserve(set.size());
    for (const auto& m : set) {
        arr.push_back(RespValue::make_bulk_string(m));
    }
    return RespValue::make_array(std::move(arr));
}

// Loads every set named in the command, or returns false if any key holds a
// non-set value (multi-key commands must fail with WRONGTYPE, not skip it).
bool load_sets(const Command& cmd, StorageEngine& engine, std::vector<std::vector<std::string>>& out) {
    out.clear();
    out.reserve(cmd.arg_count());
    for (size_t i = 0; i < cmd.arg_count(); ++i) {
        if (holds_other_type(engine, cmd.arg(i), RedisType::SET)) return false;
        out.push_back(engine.get_shard(cmd.arg(i)).smembers(cmd.arg(i)));
    }
    return true;
}

} // anonymous namespace

// SADD key member [member ...]
RespValue handle_sadd(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    int64_t added = engine.get_shard(key).sadd(key, collect_members(cmd));
    if (added == StorageShard::WRONG_TYPE) return wrongtype_error();
    if (added == StorageShard::OUT_OF_MEMORY) return oom_error();
    return RespValue::make_integer(added);
}

// SREM key member [member ...]
RespValue handle_srem(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::SET)) return wrongtype_error();
    size_t removed = engine.get_shard(key).srem(key, collect_members(cmd));
    return RespValue::make_integer(static_cast<int64_t>(removed));
}

// SISMEMBER key member
RespValue handle_sismember(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::SET)) return wrongtype_error();
    bool is_member = engine.get_shard(key).sismember(key, cmd.arg(1));
    return RespValue::make_integer(is_member ? 1 : 0);
}

// SMEMBERS key
RespValue handle_smembers(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::SET)) return wrongtype_error();
    auto members = engine.get_shard(key).smembers(key);

    if (server) {
        if (members.empty()) server->stats().record_miss();
        else server->stats().record_hit();
    }

    RespArray arr;
    arr.reserve(members.size());
    for (auto& m : members) {
        arr.push_back(RespValue::make_bulk_string(std::move(m)));
    }
    return RespValue::make_array(std::move(arr));
}

// SCARD key
RespValue handle_scard(const Command& cmd, StorageEngine& engine, RedisServer*) {
    const std::string& key = cmd.arg(0);
    if (holds_other_type(engine, key, RedisType::SET)) return wrongtype_error();
    size_t count = engine.get_shard(key).scard(key);
    return RespValue::make_integer(static_cast<int64_t>(count));
}

// SUNION key [key ...]
RespValue handle_sunion(const Command& cmd, StorageEngine& engine, RedisServer*) {
    std::vector<std::vector<std::string>> sets;
    if (!load_sets(cmd, engine, sets)) return wrongtype_error();

    std::unordered_set<std::string> union_set;
    for (auto& members : sets) {
        for (auto& m : members) {
            union_set.insert(std::move(m));
        }
    }
    return to_array(union_set);
}

// SINTER key [key ...]
RespValue handle_sinter(const Command& cmd, StorageEngine& engine, RedisServer*) {
    std::vector<std::vector<std::string>> sets;
    if (!load_sets(cmd, engine, sets)) return wrongtype_error();
    if (sets.empty()) return RespValue::make_array({});

    std::unordered_set<std::string> current_set(sets[0].begin(), sets[0].end());
    for (size_t i = 1; i < sets.size() && !current_set.empty(); ++i) {
        std::unordered_set<std::string> other_set(sets[i].begin(), sets[i].end());
        for (auto it = current_set.begin(); it != current_set.end(); ) {
            if (other_set.find(*it) == other_set.end()) {
                it = current_set.erase(it);
            } else {
                ++it;
            }
        }
    }
    return to_array(current_set);
}

// SDIFF key [key ...]
RespValue handle_sdiff(const Command& cmd, StorageEngine& engine, RedisServer*) {
    std::vector<std::vector<std::string>> sets;
    if (!load_sets(cmd, engine, sets)) return wrongtype_error();
    if (sets.empty()) return RespValue::make_array({});

    std::unordered_set<std::string> diff_set(sets[0].begin(), sets[0].end());
    for (size_t i = 1; i < sets.size() && !diff_set.empty(); ++i) {
        for (const auto& m : sets[i]) {
            diff_set.erase(m);
        }
    }
    return to_array(diff_set);
}

} // namespace miniredis
