#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <chrono>

namespace miniredis {

/**
 * @brief Thread-safe server performance counters and statistics.
 */
struct ServerStats {
    std::atomic<uint64_t> total_connections_received{0};
    std::atomic<uint64_t> active_connections{0};
    std::atomic<uint64_t> total_commands_processed{0};
    std::atomic<uint64_t> keyspace_hits{0};
    std::atomic<uint64_t> keyspace_misses{0};
    std::atomic<uint64_t> expired_keys{0};
    std::atomic<uint64_t> evicted_keys{0};
    std::atomic<uint64_t> total_net_input_bytes{0};
    std::atomic<uint64_t> total_net_output_bytes{0};
    std::atomic<int64_t>  server_start_time_sec{0};

    // Instantaneous rate metrics
    std::atomic<uint64_t> last_sample_commands{0};
    std::atomic<int64_t>  last_sample_time_ms{0};
    std::atomic<double>   instantaneous_ops_per_sec{0.0};

    void record_connection_opened() noexcept {
        total_connections_received.fetch_add(1, std::memory_order_relaxed);
        active_connections.fetch_add(1, std::memory_order_relaxed);
    }

    void record_connection_closed() noexcept {
        active_connections.fetch_sub(1, std::memory_order_relaxed);
    }

    void record_command_processed() noexcept {
        total_commands_processed.fetch_add(1, std::memory_order_relaxed);
    }

    void record_hit() noexcept {
        keyspace_hits.fetch_add(1, std::memory_order_relaxed);
    }

    void record_miss() noexcept {
        keyspace_misses.fetch_add(1, std::memory_order_relaxed);
    }

    void record_expired(uint64_t count = 1) noexcept {
        expired_keys.fetch_add(count, std::memory_order_relaxed);
    }

    void record_evicted(uint64_t count = 1) noexcept {
        evicted_keys.fetch_add(count, std::memory_order_relaxed);
    }

    void record_bytes_in(size_t bytes) noexcept {
        total_net_input_bytes.fetch_add(bytes, std::memory_order_relaxed);
    }

    void record_bytes_out(size_t bytes) noexcept {
        total_net_output_bytes.fetch_add(bytes, std::memory_order_relaxed);
    }

    void update_instantaneous_ops(int64_t now_ms) noexcept {
        int64_t last_time = last_sample_time_ms.load(std::memory_order_relaxed);
        if (last_time == 0) {
            last_sample_time_ms.store(now_ms, std::memory_order_relaxed);
            last_sample_commands.store(total_commands_processed.load(std::memory_order_relaxed), std::memory_order_relaxed);
            return;
        }

        int64_t elapsed_ms = now_ms - last_time;
        if (elapsed_ms >= 500) { // Update rate every 500ms
            uint64_t current_cmds = total_commands_processed.load(std::memory_order_relaxed);
            uint64_t last_cmds = last_sample_commands.load(std::memory_order_relaxed);
            uint64_t delta_cmds = (current_cmds >= last_cmds) ? (current_cmds - last_cmds) : 0;

            double ops = (static_cast<double>(delta_cmds) * 1000.0) / static_cast<double>(elapsed_ms);
            instantaneous_ops_per_sec.store(ops, std::memory_order_relaxed);

            last_sample_time_ms.store(now_ms, std::memory_order_relaxed);
            last_sample_commands.store(current_cmds, std::memory_order_relaxed);
        }
    }
};

} // namespace miniredis
