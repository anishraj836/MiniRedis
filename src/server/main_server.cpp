#include "server/redis_server.hpp"
#include "common/logger.hpp"
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <unistd.h>

static miniredis::RedisServer* g_server = nullptr;

// Only async-signal-safe calls are allowed here: std::cout (used before) can
// deadlock if the signal interrupts another write to the same stream.
void signal_handler(int) {
    static const char msg[] = "\nCaught signal, initiating clean shutdown...\n";
    ssize_t n = ::write(STDERR_FILENO, msg, sizeof(msg) - 1);
    (void)n;
    if (g_server) {
        g_server->stop();
    }
}

void print_help(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n\n"
              << "Options:\n"
              << "  -p, --port <port>          TCP port to listen on (default: 6379)\n"
              << "  -h, --host <ip>            IP address to bind to (default: 127.0.0.1)\n"
              << "  -t, --threads <num>        Worker reactor threads (default: 4)\n"
              << "  -s, --shards <num>         Storage shards count (default: 64)\n"
              << "  -m, --maxmemory <mb>       Maximum memory in MB (default: 256, 0=unlimited)\n"
              << "  --policy <policy>          Eviction policy (allkeys-lru, volatile-lru, allkeys-random, noeviction)\n"
              << "  --aof <yes|no>             Enable AOF persistence (default: yes)\n"
              << "  --aof-policy <policy>      AOF fsync policy (always, everysec, no) (default: everysec)\n"
              << "  --aof-file <filename>      AOF file path (default: miniredis.aof)\n"
              << "  --log-level <level>        Log level: DEBUG, INFO, WARN, ERROR (default: INFO)\n"
              << "  --help                     Display this help message\n";
}

int main(int argc, char* argv[]) {
    miniredis::ServerConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_help(argv[0]);
            return 0;
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            config.host = argv[++i];
        } else if ((arg == "-t" || arg == "--threads") && i + 1 < argc) {
            config.worker_threads = static_cast<size_t>(std::atoi(argv[++i]));
        } else if ((arg == "-s" || arg == "--shards") && i + 1 < argc) {
            config.shard_count = static_cast<size_t>(std::atoi(argv[++i]));
        } else if ((arg == "-m" || arg == "--maxmemory") && i + 1 < argc) {
            size_t mb = static_cast<size_t>(std::atol(argv[++i]));
            config.max_memory = mb * 1024 * 1024;
        } else if (arg == "--policy" && i + 1 < argc) {
            config.eviction_policy = miniredis::parse_eviction_policy(argv[++i]);
        } else if (arg == "--aof" && i + 1 < argc) {
            std::string val = argv[++i];
            config.aof_enabled = (val == "yes" || val == "true" || val == "1");
        } else if (arg == "--aof-policy" && i + 1 < argc) {
            config.aof_fsync_policy = miniredis::parse_aof_policy(argv[++i]);
        } else if (arg == "--aof-file" && i + 1 < argc) {
            config.aof_filename = argv[++i];
        } else if (arg == "--log-level" && i + 1 < argc) {
            std::string lvl = argv[++i];
            if (lvl == "DEBUG") miniredis::Logger::instance().set_level(miniredis::LogLevel::DEBUG);
            else if (lvl == "INFO") miniredis::Logger::instance().set_level(miniredis::LogLevel::INFO);
            else if (lvl == "WARN") miniredis::Logger::instance().set_level(miniredis::LogLevel::WARN);
            else if (lvl == "ERROR") miniredis::Logger::instance().set_level(miniredis::LogLevel::ERROR);
        }
    }

    miniredis::RedisServer server(config);
    g_server = &server;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    if (!server.start()) {
        return 1;
    }
    return 0;
}
