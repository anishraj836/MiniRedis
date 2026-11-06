#include "server/command_registry.hpp"
#include "server/command_helpers.hpp"
#include "server/redis_server.hpp"
#include "aof/aof_rewriter.hpp"
#include <algorithm>
#include <cctype>
#include <sstream>
#include <unistd.h>

namespace miniredis {

// PING [message]
RespValue handle_ping(const Command& cmd, StorageEngine&, RedisServer*) {
    if (cmd.arg_count() > 0) {
        return RespValue::make_bulk_string(cmd.arg(0));
    }
    return RespValue::make_pong();
}

// ECHO message
RespValue handle_echo(const Command& cmd, StorageEngine&, RedisServer*) {
    return RespValue::make_bulk_string(cmd.arg(0));
}

// TYPE key
RespValue handle_type(const Command& cmd, StorageEngine& engine, RedisServer*) {
    auto t = engine.type(cmd.arg(0));
    return RespValue::make_simple_string(t ? redis_type_to_string(*t) : "none");
}

// SHUTDOWN: stop the server; connections are closed and the AOF is flushed
// during the orderly shutdown that follows.
RespValue handle_shutdown(const Command&, StorageEngine&, RedisServer* server) {
    if (!server) return RespValue::make_error("ERR server not available");
    server->stop();
    return RespValue::make_ok();
}

// DBSIZE
RespValue handle_dbsize(const Command&, StorageEngine& engine, RedisServer*) {
    return RespValue::make_integer(static_cast<int64_t>(engine.total_keys()));
}

// FLUSHDB / FLUSHALL
RespValue handle_flushall(const Command&, StorageEngine& engine, RedisServer*) {
    engine.flush_all();
    return RespValue::make_ok();
}

// TIME
RespValue handle_time(const Command&, StorageEngine&, RedisServer*) {
    int64_t us = now_us();
    int64_t sec = us / 1000000;
    int64_t microsec = us % 1000000;

    RespArray arr;
    arr.push_back(RespValue::make_bulk_string(std::to_string(sec)));
    arr.push_back(RespValue::make_bulk_string(std::to_string(microsec)));
    return RespValue::make_array(std::move(arr));
}

// INFO [section]
RespValue handle_info(const Command&, StorageEngine& engine, RedisServer* server) {
    std::ostringstream oss;
    oss << "# Server\r\n";
    oss << "miniredis_version:1.0.0\r\n";
    oss << "os:macOS/POSIX\r\n";
    oss << "arch_bits:64\r\n";
    oss << "process_id:" << getpid() << "\r\n";
    oss << "uptime_in_seconds:" << (server ? (now_ms() / 1000 - server->stats().server_start_time_sec.load()) : 0) << "\r\n";

    oss << "\r\n# Clients\r\n";
    oss << "connected_clients:" << (server ? server->stats().active_connections.load() : 0) << "\r\n";

    oss << "\r\n# Memory\r\n";
    oss << "used_memory:" << engine.memory_tracker().get_used_memory() << "\r\n";
    oss << "used_memory_human:" << engine.memory_tracker().format_memory_human() << "\r\n";
    oss << "maxmemory:" << engine.memory_tracker().get_max_memory() << "\r\n";
    oss << "maxmemory_policy:" << to_string(engine.memory_tracker().get_eviction_policy()) << "\r\n";

    oss << "\r\n# Persistence\r\n";
    oss << "aof_enabled:" << (server && server->config().aof_enabled ? 1 : 0) << "\r\n";
    oss << "aof_rewrite_in_progress:" << (AofRewriter::is_rewrite_in_progress() ? 1 : 0) << "\r\n";

    oss << "\r\n# Stats\r\n";
    if (server) {
        oss << "total_connections_received:" << server->stats().total_connections_received.load() << "\r\n";
        oss << "total_commands_processed:" << server->stats().total_commands_processed.load() << "\r\n";
        oss << "instantaneous_ops_per_sec:" << static_cast<uint64_t>(server->stats().instantaneous_ops_per_sec.load()) << "\r\n";
        oss << "keyspace_hits:" << server->stats().keyspace_hits.load() << "\r\n";
        oss << "keyspace_misses:" << server->stats().keyspace_misses.load() << "\r\n";
        oss << "expired_keys:" << server->stats().expired_keys.load() << "\r\n";
    }
    oss << "evicted_keys:" << engine.memory_tracker().evicted_keys() << "\r\n";

    oss << "\r\n# Keyspace\r\n";
    oss << "db0:keys=" << engine.total_keys() << ",expires=" << engine.total_expires() << "\r\n";

    return RespValue::make_bulk_string(oss.str());
}

// SAVE
RespValue handle_save(const Command&, StorageEngine& engine, RedisServer* server) {
    if (!server) return RespValue::make_error("ERR server not available");
    bool ok = AofRewriter::rewrite_synchronous(engine, server->aof_logger(), server->config().aof_filename,
                                               &server->key_locks());
    if (!ok) return RespValue::make_error("ERR save failed or a rewrite is already in progress");
    return RespValue::make_ok();
}

// BGSAVE / BGREWRITEAOF
RespValue handle_bgrewriteaof(const Command&, StorageEngine& engine, RedisServer* server) {
    if (!server) return RespValue::make_error("ERR server not available");
    bool ok = AofRewriter::start_background_rewrite(engine, server->aof_logger(), server->config().aof_filename,
                                                    &server->key_locks());
    if (!ok) return RespValue::make_error("ERR background rewrite already in progress or failed");
    return RespValue::make_simple_string("Background append only file rewriting started");
}

// CONFIG GET parameter | CONFIG SET parameter value
RespValue handle_config(const Command& cmd, StorageEngine& engine, RedisServer*) {
    auto lower = [](std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
        return s;
    };
    const std::string sub = lower(cmd.arg(0));
    auto& mem = engine.memory_tracker();

    if (sub == "get" && cmd.arg_count() == 2) {
        const std::string param = lower(cmd.arg(1));
        RespArray arr;
        if (param == "maxmemory" || param == "*") {
            arr.push_back(RespValue::make_bulk_string("maxmemory"));
            arr.push_back(RespValue::make_bulk_string(std::to_string(mem.get_max_memory())));
        }
        if (param == "maxmemory-policy" || param == "*") {
            arr.push_back(RespValue::make_bulk_string("maxmemory-policy"));
            arr.push_back(RespValue::make_bulk_string(to_string(mem.get_eviction_policy())));
        }
        return RespValue::make_array(std::move(arr));
    }

    if (sub == "set" && cmd.arg_count() == 3) {
        // Previously CONFIG SET replied +OK and silently changed nothing.
        const std::string param = lower(cmd.arg(1));
        const std::string value = lower(cmd.arg(2));
        if (param == "maxmemory") {
            int64_t bytes = 0;
            if (!parse_int64_strict(value, bytes) || bytes < 0) {
                return RespValue::make_error("ERR Invalid argument '" + cmd.arg(2) + "' for CONFIG SET 'maxmemory'");
            }
            mem.set_max_memory(static_cast<size_t>(bytes));
            return RespValue::make_ok();
        }
        if (param == "maxmemory-policy") {
            if (value != "noeviction" && value != "allkeys-lru" && value != "volatile-lru" &&
                value != "allkeys-random" && value != "volatile-ttl") {
                return RespValue::make_error("ERR Invalid argument '" + cmd.arg(2) + "' for CONFIG SET 'maxmemory-policy'");
            }
            mem.set_eviction_policy(parse_eviction_policy(value));
            return RespValue::make_ok();
        }
        return RespValue::make_error("ERR Unsupported CONFIG parameter: " + cmd.arg(1));
    }

    return RespValue::make_error("ERR Unknown CONFIG subcommand or wrong number of arguments for '" + cmd.arg(0) + "'");
}

} // namespace miniredis
