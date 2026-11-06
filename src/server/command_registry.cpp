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
RespValue handle_incrby(const Command&, StorageEngine&, RedisServer*);
RespValue handle_decrby(const Command&, StorageEngine&, RedisServer*);
RespValue handle_type(const Command&, StorageEngine&, RedisServer*);
RespValue handle_shutdown(const Command&, StorageEngine&, RedisServer*);

namespace {
constexpr KeySpec ONE_KEY{1, 1, 1};
constexpr KeySpec ALL_ARGS_KEYS{1, -1, 1};
constexpr KeySpec EVERY_OTHER_KEY{1, -1, 2};
constexpr KeySpec WHOLE_KEYSPACE{KeySpec::ALL_KEYS, 0, 1};
} // anonymous namespace

CommandRegistry::CommandRegistry() {
    register_all_commands();
}

void CommandRegistry::register_command(std::string name, int arity, CommandFlag flags, CommandHandler handler,
                                       KeySpec keys) {
    std::string upper = name;
    std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) { return std::toupper(c); });
    commands_[upper] = {std::move(name), arity, flags, std::move(handler), keys};
}

std::vector<std::string_view> CommandRegistry::keys_of(const CommandMeta& meta, const Command& cmd) {
    std::vector<std::string_view> keys;
    const auto& tokens = cmd.tokens();
    const int n = static_cast<int>(tokens.size());
    if (meta.keys.first_key <= 0 || meta.keys.key_step <= 0) {
        return keys;
    }
    const int last = meta.keys.last_key < 0 ? n + meta.keys.last_key : meta.keys.last_key;
    for (int i = meta.keys.first_key; i <= last && i < n; i += meta.keys.key_step) {
        keys.emplace_back(tokens[static_cast<size_t>(i)]);
    }
    return keys;
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
    register_command("GET", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_get, ONE_KEY);
    register_command("SET", -3, CommandFlag::WRITE, handle_set, ONE_KEY);
    register_command("SETEX", 4, CommandFlag::WRITE, handle_setex, ONE_KEY);
    register_command("SETNX", 3, CommandFlag::WRITE, handle_setnx, ONE_KEY);
    register_command("DEL", -2, CommandFlag::WRITE, handle_del, ALL_ARGS_KEYS);
    register_command("EXISTS", -2, CommandFlag::READONLY, handle_exists, ALL_ARGS_KEYS);
    register_command("INCR", 2, CommandFlag::WRITE, handle_incr, ONE_KEY);
    register_command("DECR", 2, CommandFlag::WRITE, handle_decr, ONE_KEY);
    register_command("INCRBY", 3, CommandFlag::WRITE, handle_incrby, ONE_KEY);
    register_command("DECRBY", 3, CommandFlag::WRITE, handle_decrby, ONE_KEY);
    register_command("MGET", -2, CommandFlag::READONLY, handle_mget, ALL_ARGS_KEYS);
    register_command("MSET", -3, CommandFlag::WRITE, handle_mset, EVERY_OTHER_KEY);
    register_command("APPEND", 3, CommandFlag::WRITE, handle_append, ONE_KEY);
    register_command("STRLEN", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_strlen, ONE_KEY);
    register_command("TYPE", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_type, ONE_KEY);

    // Hashes
    register_command("HSET", -4, CommandFlag::WRITE, handle_hset, ONE_KEY);
    register_command("HGET", 3, CommandFlag::READONLY | CommandFlag::FAST, handle_hget, ONE_KEY);
    register_command("HDEL", -3, CommandFlag::WRITE, handle_hdel, ONE_KEY);
    register_command("HGETALL", 2, CommandFlag::READONLY, handle_hgetall, ONE_KEY);
    register_command("HEXISTS", 3, CommandFlag::READONLY | CommandFlag::FAST, handle_hexists, ONE_KEY);
    register_command("HLEN", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_hlen, ONE_KEY);
    register_command("HKEYS", 2, CommandFlag::READONLY, handle_hkeys, ONE_KEY);
    register_command("HVALS", 2, CommandFlag::READONLY, handle_hvals, ONE_KEY);
    register_command("HMGET", -3, CommandFlag::READONLY, handle_hmget, ONE_KEY);
    register_command("HMSET", -4, CommandFlag::WRITE, handle_hmset, ONE_KEY);
    register_command("HINCRBY", 4, CommandFlag::WRITE, handle_hincrby, ONE_KEY);

    // Lists
    register_command("LPUSH", -3, CommandFlag::WRITE, handle_lpush, ONE_KEY);
    register_command("RPUSH", -3, CommandFlag::WRITE, handle_rpush, ONE_KEY);
    register_command("LPOP", 2, CommandFlag::WRITE, handle_lpop, ONE_KEY);
    register_command("RPOP", 2, CommandFlag::WRITE, handle_rpop, ONE_KEY);
    register_command("LLEN", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_llen, ONE_KEY);
    register_command("LRANGE", 4, CommandFlag::READONLY, handle_lrange, ONE_KEY);

    // Sets
    register_command("SADD", -3, CommandFlag::WRITE, handle_sadd, ONE_KEY);
    register_command("SREM", -3, CommandFlag::WRITE, handle_srem, ONE_KEY);
    register_command("SISMEMBER", 3, CommandFlag::READONLY | CommandFlag::FAST, handle_sismember, ONE_KEY);
    register_command("SMEMBERS", 2, CommandFlag::READONLY, handle_smembers, ONE_KEY);
    register_command("SCARD", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_scard, ONE_KEY);
    register_command("SUNION", -2, CommandFlag::READONLY, handle_sunion, ALL_ARGS_KEYS);
    register_command("SINTER", -2, CommandFlag::READONLY, handle_sinter, ALL_ARGS_KEYS);
    register_command("SDIFF", -2, CommandFlag::READONLY, handle_sdiff, ALL_ARGS_KEYS);

    // TTL / Expiration
    register_command("EXPIRE", 3, CommandFlag::WRITE, handle_expire, ONE_KEY);
    register_command("PEXPIRE", 3, CommandFlag::WRITE, handle_pexpire, ONE_KEY);
    register_command("TTL", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_ttl, ONE_KEY);
    register_command("PTTL", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_pttl, ONE_KEY);
    register_command("PERSIST", 2, CommandFlag::WRITE, handle_persist, ONE_KEY);
    register_command("EXPIREAT", 3, CommandFlag::WRITE, handle_expireat, ONE_KEY);
    register_command("PEXPIREAT", 3, CommandFlag::WRITE, handle_pexpireat, ONE_KEY);

    // Server / Admin
    register_command("PING", -1, CommandFlag::READONLY | CommandFlag::FAST, handle_ping);
    register_command("ECHO", 2, CommandFlag::READONLY | CommandFlag::FAST, handle_echo);
    register_command("DBSIZE", 1, CommandFlag::READONLY | CommandFlag::FAST, handle_dbsize);
    register_command("FLUSHDB", 1, CommandFlag::WRITE | CommandFlag::ADMIN, handle_flushall, WHOLE_KEYSPACE);
    register_command("FLUSHALL", 1, CommandFlag::WRITE | CommandFlag::ADMIN, handle_flushall, WHOLE_KEYSPACE);
    register_command("TIME", 1, CommandFlag::READONLY | CommandFlag::FAST, handle_time);
    register_command("INFO", -1, CommandFlag::READONLY, handle_info);
    register_command("SAVE", 1, CommandFlag::ADMIN, handle_save);
    register_command("BGSAVE", 1, CommandFlag::ADMIN, handle_bgrewriteaof);
    register_command("BGREWRITEAOF", 1, CommandFlag::ADMIN, handle_bgrewriteaof);
    register_command("CONFIG", -2, CommandFlag::ADMIN, handle_config);
    register_command("SHUTDOWN", -1, CommandFlag::ADMIN, handle_shutdown);
}

} // namespace miniredis
