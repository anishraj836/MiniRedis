#include "server/command_registry.hpp"

namespace miniredis {

// Forward declarations of command handlers
RespValue handle_get(const Command&, StorageEngine&, RedisServer*);
RespValue handle_set(const Command&, StorageEngine&, RedisServer*);
RespValue handle_setex(const Command&, StorageEngine&, RedisServer*);
RespValue handle_setnx(const Command&, StorageEngine&, RedisServer*);
RespValue handle_del(const Command&, StorageEngine&, RedisServer*);
RespValue handle_exists(const Command&, StorageEngine&, RedisServer*);
RespValue handle_incr(const Command&, StorageEngine&, RedisServer*);
RespValue handle_decr(const Command&, StorageEngine&, RedisServer*);
RespValue handle_mget(const Command&, StorageEngine&, RedisServer*);
RespValue handle_mset(const Command&, StorageEngine&, RedisServer*);
RespValue handle_append(const Command&, StorageEngine&, RedisServer*);
RespValue handle_strlen(const Command&, StorageEngine&, RedisServer*);

RespValue handle_hset(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hget(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hdel(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hgetall(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hexists(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hlen(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hkeys(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hvals(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hmget(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hmset(const Command&, StorageEngine&, RedisServer*);
RespValue handle_hincrby(const Command&, StorageEngine&, RedisServer*);

RespValue handle_lpush(const Command&, StorageEngine&, RedisServer*);
RespValue handle_rpush(const Command&, StorageEngine&, RedisServer*);
RespValue handle_lpop(const Command&, StorageEngine&, RedisServer*);
RespValue handle_rpop(const Command&, StorageEngine&, RedisServer*);
RespValue handle_llen(const Command&, StorageEngine&, RedisServer*);
RespValue handle_lrange(const Command&, StorageEngine&, RedisServer*);

RespValue handle_sadd(const Command&, StorageEngine&, RedisServer*);
RespValue handle_srem(const Command&, StorageEngine&, RedisServer*);
RespValue handle_sismember(const Command&, StorageEngine&, RedisServer*);
RespValue handle_smembers(const Command&, StorageEngine&, RedisServer*);
RespValue handle_scard(const Command&, StorageEngine&, RedisServer*);
RespValue handle_sunion(const Command&, StorageEngine&, RedisServer*);
RespValue handle_sinter(const Command&, StorageEngine&, RedisServer*);
RespValue handle_sdiff(const Command&, StorageEngine&, RedisServer*);

RespValue handle_expire(const Command&, StorageEngine&, RedisServer*);
RespValue handle_pexpire(const Command&, StorageEngine&, RedisServer*);
RespValue handle_ttl(const Command&, StorageEngine&, RedisServer*);
RespValue handle_pttl(const Command&, StorageEngine&, RedisServer*);
RespValue handle_persist(const Command&, StorageEngine&, RedisServer*);
RespValue handle_expireat(const Command&, StorageEngine&, RedisServer*);
RespValue handle_pexpireat(const Command&, StorageEngine&, RedisServer*);

RespValue handle_ping(const Command&, StorageEngine&, RedisServer*);
RespValue handle_echo(const Command&, StorageEngine&, RedisServer*);
RespValue handle_dbsize(const Command&, StorageEngine&, RedisServer*);
RespValue handle_flushall(const Command&, StorageEngine&, RedisServer*);
RespValue handle_time(const Command&, StorageEngine&, RedisServer*);
RespValue handle_info(const Command&, StorageEngine&, RedisServer*);
RespValue handle_save(const Command&, StorageEngine&, RedisServer*);
RespValue handle_bgrewriteaof(const Command&, StorageEngine&, RedisServer*);
RespValue handle_config(const Command&, StorageEngine&, RedisServer*);

CommandRegistry::CommandRegistry() {
    register_all_commands();
}

void CommandRegistry::register_command(std::string name, int arity, CommandFlag flags, CommandHandler handler) {
    std::string upper = name;
    std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) { return std::toupper(c); });
    commands_[upper] = {std::move(name), arity, flags, std::move(handler)};
}

const CommandMeta* CommandRegistry::find_command(const std::string& upper_name) const {
    auto it = commands_.find(upper_name);
    if (it != commands_.end()) {
        return &it->second;
    }
    return nullptr;
}

RespValue CommandRegistry::dispatch(const Command& cmd, StorageEngine& engine, RedisServer* server) {
    if (cmd.empty()) {
        return RespValue::make_error("ERR empty command");
    }

    const auto* meta = find_command(cmd.upper_name());
    if (!meta) {
        return RespValue::make_error("ERR unknown command '" + cmd.name() + "'");
    }

    // Check arity
    int num_tokens = static_cast<int>(cmd.tokens().size());
    if (meta->arity > 0 && num_tokens != meta->arity) {
        return RespValue::make_error("ERR wrong number of arguments for '" + cmd.name() + "' command");
    } else if (meta->arity < 0 && num_tokens < -meta->arity) {
        return RespValue::make_error("ERR wrong number of arguments for '" + cmd.name() + "' command");
    }

    return meta->handler(cmd, engine, server);
}

void CommandRegistry::register_all_commands() {
    // Strings
    register_command("GET", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_get);
    register_command("SET", -3, CommandFlag::WRITE, handle_set);
    register_command("SETEX", 4, CommandFlag::WRITE, handle_setex);
    register_command("SETNX", 3, CommandFlag::WRITE, handle_setnx);
    register_command("DEL", -2, CommandFlag::WRITE, handle_del);
    register_command("EXISTS", -2, CommandFlag::READONLY, handle_exists);
    register_command("INCR", 2, CommandFlag::WRITE, handle_incr);
    register_command("DECR", 2, CommandFlag::WRITE, handle_decr);
    register_command("MGET", -2, CommandFlag::READONLY, handle_mget);
    register_command("MSET", -3, CommandFlag::WRITE, handle_mset);
    register_command("APPEND", 3, CommandFlag::WRITE, handle_append);
    register_command("STRLEN", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_strlen);

    // Hashes
    register_command("HSET", -4, CommandFlag::WRITE, handle_hset);
    register_command("HGET", 3, CommandFlag::READONLY | CommandFlag::FAST, handle_hget);
    register_command("HDEL", -3, CommandFlag::WRITE, handle_hdel);
    register_command("HGETALL", 2, CommandFlag::READONLY, handle_hgetall);
    register_command("HEXISTS", 3, CommandFlag::READONLY | CommandFlag::FAST, handle_hexists);
    register_command("HLEN", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_hlen);
    register_command("HKEYS", 2, CommandFlag::READONLY, handle_hkeys);
    register_command("HVALS", 2, CommandFlag::READONLY, handle_hvals);
    register_command("HMGET", -3, CommandFlag::READONLY, handle_hmget);
    register_command("HMSET", -4, CommandFlag::WRITE, handle_hmset);
    register_command("HINCRBY", 4, CommandFlag::WRITE, handle_hincrby);

    // Lists
    register_command("LPUSH", -3, CommandFlag::WRITE, handle_lpush);
    register_command("RPUSH", -3, CommandFlag::WRITE, handle_rpush);
    register_command("LPOP", 2, CommandFlag::WRITE, handle_lpop);
    register_command("RPOP", 2, CommandFlag::WRITE, handle_rpop);
    register_command("LLEN", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_llen);
    register_command("LRANGE", 4, CommandFlag::READONLY, handle_lrange);

    // Sets
    register_command("SADD", -3, CommandFlag::WRITE, handle_sadd);
    register_command("SREM", -3, CommandFlag::WRITE, handle_srem);
    register_command("SISMEMBER", 3, CommandFlag::READONLY | CommandFlag::FAST, handle_sismember);
    register_command("SMEMBERS", 2, CommandFlag::READONLY, handle_smembers);
    register_command("SCARD", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_scard);
    register_command("SUNION", -2, CommandFlag::READONLY, handle_sunion);
    register_command("SINTER", -2, CommandFlag::READONLY, handle_sinter);
    register_command("SDIFF", -2, CommandFlag::READONLY, handle_sdiff);

    // TTL / Expiration
    register_command("EXPIRE", 3, CommandFlag::WRITE, handle_expire);
    register_command("PEXPIRE", 3, CommandFlag::WRITE, handle_pexpire);
    register_command("TTL", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_ttl);
    register_command("PTTL", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_pttl);
    register_command("PERSIST", 2, CommandFlag::WRITE, handle_persist);
    register_command("EXPIREAT", 3, CommandFlag::WRITE, handle_expireat);
    register_command("PEXPIREAT", 3, CommandFlag::WRITE, handle_pexpireat);

    // Server / Admin
    register_command("PING", -1, CommandFlag::READONLY | CommandFlag::FAST, handle_ping);
    register_command("ECHO", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_echo);
    register_command("DBSIZE", 1, CommandFlag::READONLY | CommandFlag::FAST, handle_dbsize);
    register_command("FLUSHDB", 1, CommandFlag::WRITE | CommandFlag::ADMIN, handle_flushall);
    register_command("FLUSHALL", 1, CommandFlag::WRITE | CommandFlag::ADMIN, handle_flushall);
    register_command("TIME", 1, CommandFlag::READONLY | CommandFlag::FAST, handle_time);
    register_command("INFO", -1, CommandFlag::READONLY, handle_info);
    register_command("SAVE", 1, CommandFlag::ADMIN, handle_save);
    register_command("BGSAVE", 1, CommandFlag::ADMIN, handle_bgrewriteaof);
    register_command("BGREWRITEAOF", 1, CommandFlag::ADMIN, handle_bgrewriteaof);
    register_command("CONFIG", -2, CommandFlag::ADMIN, handle_config);
}

} // namespace miniredis
