#include "server/redis_server.hpp"
#include "server/command_rewrite.hpp"
#include "aof/aof_recovery.hpp"
#include "aof/aof_rewriter.hpp"
#include "common/logger.hpp"
#include <csignal>
#include <optional>

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

    // Replay AOF file before opening port to incoming traffic. If the file is
    // corrupted, refuse to start rather than serving (and then appending to) a
    // partial dataset.
    if (config_.aof_enabled) {
        size_t replayed = 0;
        if (!AofRecovery::replay_file(config_.aof_filename, storage_engine_, command_registry_, replayed)) {
            LOG_FATAL("Could not load AOF file " + config_.aof_filename + "; not starting.");
            return false;
        }
        if (!aof_logger_.open()) {
            LOG_FATAL("Could not open AOF file " + config_.aof_filename + " for writing; not starting.");
            return false;
        }
    }

    // Configure TCP server hooks
    tcp_server_.set_connection_callback([this](const ConnectionPtr& conn) {
        on_connection(conn);
    });

    tcp_server_.set_message_callback([this](const ConnectionPtr& conn, ByteBuffer& buffer) {
        on_message(conn, buffer);
    });

    // Registered on the server, not on each Connection: Connection::set_close_callback
    // would overwrite the TcpServer hook that releases the socket, leaking
    // one file descriptor per client until the process hit its fd limit.
    tcp_server_.set_close_callback([this](const ConnectionPtr& c) {
        stats_.record_connection_closed();
        LOG_DEBUG("Connection closed from " + c->peer_address() + " [Total active: " + std::to_string(stats_.active_connections.load()) + "]");
    });

    // Schedule active TTL sweeper (100ms interval)
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
    shutdown();
    return true;
}

void RedisServer::stop() {
    // Only signals the main loop to exit; the actual teardown runs on the loop
    // thread in shutdown(). This keeps stop() async-signal-safe (it is called
    // from the SIGINT/SIGTERM handler) and avoids tearing down sockets and I/O
    // threads while the main loop is still accepting connections on them.
    running_.store(false);
    main_loop_.stop();
}

void RedisServer::shutdown() {
    LOG_INFO("Stopping MiniRedis server...");
    tcp_server_.stop();
    // A background rewrite holds references to the engine and the logger.
    AofRewriter::wait_for_background_rewrite();
    if (config_.aof_enabled) {
        aof_logger_.close();
    }
    LOG_INFO("MiniRedis server stopped cleanly.");
}

void RedisServer::on_connection(const ConnectionPtr& conn) {
    stats_.record_connection_opened();
    LOG_DEBUG("New connection from " + conn->peer_address() + " [Total active: " + std::to_string(stats_.active_connections.load()) + "]");
}

RespValue RedisServer::execute(Command cmd) {
    const CommandMeta* meta = command_registry_.find_command(cmd.upper_name());
    if (!meta || !(meta->flags & CommandFlag::WRITE)) {
        return command_registry_.dispatch(cmd, storage_engine_, this);
    }

    // Turn relative TTLs into absolute deadlines so the logged command replays
    // to the same state, and run the rewritten form so both always agree.
    if (auto normalized = normalize_for_propagation(cmd.tokens(), now_ms())) {
        cmd = Command(std::move(*normalized));
        meta = command_registry_.find_command(cmd.upper_name());
        if (!meta) {
            return RespValue::make_error("ERR internal error while rewriting command");
        }
    }

    // Hold the write locks of every key the command touches across execution
    // and the AOF append. This makes read-modify-write commands atomic and
    // keeps the AOF in the same order the writes were applied.
    std::optional<KeyLockTable::Guard> guard;
    if (meta->keys.first_key == KeySpec::ALL_KEYS) {
        guard.emplace(key_locks_.lock_all());
    } else {
        guard.emplace(key_locks_.lock_keys(CommandRegistry::keys_of(*meta, cmd)));
    }

    RespValue resp = command_registry_.dispatch(cmd, storage_engine_, this);

    // Failed commands changed nothing; logging them would only replay errors
    // (or, for an OOM-rejected write, apply it on restart when it never
    // happened live).
    if (config_.aof_enabled && resp.type != RespType::ERROR) {
        aof_logger_.append(cmd.tokens());
    }
    return resp;
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

            if (cmd.upper_name() == "QUIT") {
                // Replies to earlier pipelined commands must still go out
                // (they used to be dropped).
                response_batch.append(RespSerializer::serialize_ok());
                stats_.record_bytes_out(response_batch.size());
                conn->send(response_batch);
                conn->shutdown();
                return;
            }

            RespSerializer::serialize(execute(std::move(cmd)), response_batch);
        } else if (res == ParseResult::INCOMPLETE) {
            break; // Await more network bytes
        } else {
            // The stream is no longer frame-aligned, so nothing after this
            // point can be parsed reliably: report the error and close, as
            // Redis does.
            response_batch.append("-ERR Protocol error: unknown or invalid frame\r\n");
            buffer.retrieve_all();
            stats_.record_bytes_out(response_batch.size());
            conn->send(response_batch);
            conn->shutdown();
            return;
        }
    }

    if (!response_batch.empty()) {
        stats_.record_bytes_out(response_batch.size());
        conn->send(response_batch);
    }
}

std::string RedisServer::execute_command_tokens(const std::vector<std::string>& tokens) {
    return RespSerializer::serialize(execute(Command(tokens)));
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
