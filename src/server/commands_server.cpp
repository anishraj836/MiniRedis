#include "server/command_registry.hpp"
#include "server/redis_server.hpp"
#include "aof/aof_rewriter.hpp"
#include <sstream>

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
        oss << "evicted_keys:" << server->stats().evicted_keys.load() << "\r\n";
    }

    oss << "\r\n# Keyspace\r\n";
    oss << "db0:keys=" << engine.total_keys() << ",expires=" << 0 << "\r\n";

    return RespValue::make_bulk_string(oss.str());
}

// SAVE
RespValue handle_save(const Command&, StorageEngine& engine, RedisServer* server) {
    if (!server) return RespValue::make_error("ERR server not available");
    bool ok = AofRewriter::rewrite_synchronous(engine, server->aof_logger(), server->config().aof_filename);
    if (!ok) return RespValue::make_error("ERR save failed");
    return RespValue::make_ok();
}

// BGSAVE / BGREWRITEAOF
RespValue handle_bgrewriteaof(const Command&, StorageEngine& engine, RedisServer* server) {
    if (!server) return RespValue::make_error("ERR server not available");
    bool ok = AofRewriter::start_background_rewrite(engine, server->aof_logger(), server->config().aof_filename);
    if (!ok) return RespValue::make_error("ERR background rewrite already in progress or failed");
    return RespValue::make_simple_string("Background append only file rewriting started");
}

// CONFIG GET / CONFIG SET
RespValue handle_config(const Command& cmd, StorageEngine& engine, RedisServer*) {
    if (cmd.arg_count() >= 2 && (cmd.arg(0) == "GET" || cmd.arg(0) == "get")) {
        std::string param = cmd.arg(1);
        RespArray arr;
        if (param == "maxmemory" || param == "*") {
            arr.push_back(RespValue::make_bulk_string("maxmemory"));
            arr.push_back(RespValue::make_bulk_string(std::to_string(engine.memory_tracker().get_max_memory())));
        }
        if (param == "maxmemory-policy" || param == "*") {
            arr.push_back(RespValue::make_bulk_string("maxmemory-policy"));
            arr.push_back(RespValue::make_bulk_string(to_string(engine.memory_tracker().get_eviction_policy())));
        }
        return RespValue::make_array(std::move(arr));
    }
    return RespValue::make_ok();
}

} // namespace miniredis
