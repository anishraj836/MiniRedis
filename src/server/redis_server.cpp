#include "server/redis_server.hpp"
#include "aof/aof_recovery.hpp"
#include "common/logger.hpp"
#include <csignal>

namespace miniredis {

RedisServer::RedisServer(ServerConfig config)
    : config_(std::move(config)),
      tcp_server_(&main_loop_, config_.host, config_.port, config_.worker_threads),
      storage_engine_(config_.shard_count, config_.max_memory, config_.eviction_policy),
      aof_logger_(config_.aof_filename, config_.aof_fsync_policy) {
    stats_.server_start_time_sec.store(now_ms() / 1000, std::memory_order_relaxed);
}

RedisServer::~RedisServer() {
    stop();
}

bool RedisServer::start() {
    // Ignore SIGPIPE to avoid crash on write to closed socket
    ::signal(SIGPIPE, SIG_IGN);

    LOG_INFO("==================================================================");
    LOG_INFO("              MiniRedis: In-Memory Key-Value Store               ");
    LOG_INFO("==================================================================");
    LOG_INFO("Port: " + std::to_string(config_.port) + " | Host: " + config_.host);
    LOG_INFO("Reactor Threads: " + std::to_string(config_.worker_threads));
    LOG_INFO("Storage Shards: " + std::to_string(config_.shard_count));
    LOG_INFO("Max Memory: " + storage_engine_.memory_tracker().format_memory_human() + " (" + to_string(config_.eviction_policy) + ")");

    // Replay AOF file before opening port to incoming traffic
    if (config_.aof_enabled) {
        size_t replayed = 0;
        AofRecovery::replay_file(config_.aof_filename, storage_engine_, command_registry_, replayed);
        aof_logger_.open();
    }

    // Configure TCP server hooks
    tcp_server_.set_connection_callback([this](const ConnectionPtr& conn) {
        on_connection(conn);
    });

    tcp_server_.set_message_callback([this](const ConnectionPtr& conn, ByteBuffer& buffer) {
        on_message(conn, buffer);
    });

    // Schedule active probabilistic TTL sweeper (100ms interval)
    main_loop_.run_every(100, [this]() {
        periodic_ttl_cycle();
    });

    // Schedule stats ops/sec updater (500ms interval)
    main_loop_.run_every(500, [this]() {
        periodic_stats_update();
    });

    running_ = true;
    if (!tcp_server_.start()) {
        LOG_FATAL("Failed to start TCP server on " + config_.host + ":" + std::to_string(config_.port));
        return false;
    }

    main_loop_.loop();
    return true;
}

void RedisServer::stop() {
    if (!running_.exchange(false)) return;

    LOG_INFO("Stopping MiniRedis server...");
    tcp_server_.stop();
    if (config_.aof_enabled) {
        aof_logger_.close();
    }
    main_loop_.stop();
    LOG_INFO("MiniRedis server stopped cleanly.");
}

void RedisServer::on_connection(const ConnectionPtr& conn) {
    stats_.record_connection_opened();
    LOG_DEBUG("New connection from " + conn->peer_address() + " [Total active: " + std::to_string(stats_.active_connections.load()) + "]");

    conn->set_close_callback([this](const ConnectionPtr& c) {
        stats_.record_connection_closed();
        LOG_DEBUG("Connection closed from " + c->peer_address() + " [Total active: " + std::to_string(stats_.active_connections.load()) + "]");
    });
}

void RedisServer::on_message(const ConnectionPtr& conn, ByteBuffer& buffer) {
    stats_.record_bytes_in(buffer.readable_bytes());

    RespParser parser;
    std::string response_batch;

    while (true) {
        std::vector<std::string> cmd_tokens;
        ParseResult res = parser.parse_command(buffer, cmd_tokens);

        if (res == ParseResult::OK) {
            Command cmd(std::move(cmd_tokens));
            stats_.record_command_processed();

            const auto* meta = command_registry_.find_command(cmd.upper_name());
            RespValue resp = command_registry_.dispatch(cmd, storage_engine_, this);

            // If command is a mutating write operation and AOF is enabled, log it
            if (config_.aof_enabled && meta && (meta->flags & CommandFlag::WRITE)) {
                aof_logger_.append(cmd.tokens());
            }

            // Check QUIT command
            if (cmd.upper_name() == "QUIT") {
                conn->send(RespSerializer::serialize_ok());
                conn->shutdown();
                return;
            }

            RespSerializer::serialize(resp, response_batch);
        } else if (res == ParseResult::INCOMPLETE) {
            break; // Await more network bytes
        } else {
            // Protocol syntax error
            response_batch.append("-ERR Protocol error: unknown or invalid frame\r\n");
            buffer.retrieve_all();
            break;
        }
    }

    if (!response_batch.empty()) {
        stats_.record_bytes_out(response_batch.size());
        conn->send(response_batch);
    }
}

std::string RedisServer::execute_command_tokens(const std::vector<std::string>& tokens) {
    Command cmd(tokens);
    RespValue resp = command_registry_.dispatch(cmd, storage_engine_, this);
    return RespSerializer::serialize(resp);
}

void RedisServer::periodic_ttl_cycle() {
    size_t swept = storage_engine_.active_expire_cycle();
    if (swept > 0) {
        stats_.record_expired(swept);
    }
}

void RedisServer::periodic_stats_update() {
    stats_.update_instantaneous_ops(now_ms());
}

} // namespace miniredis
