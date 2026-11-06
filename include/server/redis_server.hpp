#pragma once

#include "server/server_config.hpp"
#include "net/tcp_server.hpp"
#include "net/event_loop.hpp"
#include "storage/storage_engine.hpp"
#include "protocol/resp_parser.hpp"
#include "protocol/resp_serializer.hpp"
#include "server/command_registry.hpp"
#include "aof/aof_logger.hpp"
#include "common/atomic_stats.hpp"
#include "common/key_lock_table.hpp"
#include <memory>
#include <atomic>

namespace miniredis {

class RedisServer {
public:
    explicit RedisServer(ServerConfig config);
    ~RedisServer();

    RedisServer(const RedisServer&) = delete;
    RedisServer& operator=(const RedisServer&) = delete;

    // Runs the server on the calling thread until stop() is called, then shuts
    // down (closes connections, waits for a running rewrite, flushes the AOF).
    bool start();
    // Asks a running server to stop. Safe to call from any thread and from a
    // signal handler.
    void stop();

    [[nodiscard]] StorageEngine& storage_engine() noexcept { return storage_engine_; }
    [[nodiscard]] CommandRegistry& command_registry() noexcept { return command_registry_; }
    [[nodiscard]] AofLogger& aof_logger() noexcept { return aof_logger_; }
    [[nodiscard]] ServerStats& stats() noexcept { return stats_; }
    [[nodiscard]] const ServerConfig& config() const noexcept { return config_; }
    [[nodiscard]] KeyLockTable& key_locks() noexcept { return key_locks_; }

    // Execute raw command string tokens and return formatted RESP string
    std::string execute_command_tokens(const std::vector<std::string>& tokens);

    // Runs one command. Write commands are normalized for replay, executed
    // under their keys' write locks and appended to the AOF before the locks
    // are released.
    RespValue execute(Command cmd);

private:
    void shutdown();
    void on_connection(const ConnectionPtr& conn);
    void on_message(const ConnectionPtr& conn, ByteBuffer& buffer);
    void periodic_ttl_cycle();
    void periodic_stats_update();

    ServerConfig config_;
    EventLoop main_loop_;
    TcpServer tcp_server_;
    StorageEngine storage_engine_;
    CommandRegistry command_registry_;
    AofLogger aof_logger_;
    KeyLockTable key_locks_;
    ServerStats stats_;
    std::atomic<bool> running_{false};
};

} // namespace miniredis
