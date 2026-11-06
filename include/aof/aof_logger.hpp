#pragma once

#include "common/types.hpp"
#include "common/byte_buffer.hpp"
#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

namespace miniredis {

/**
 * @brief Append-only file writer.
 *
 * Writers append RESP-encoded commands to an in-memory buffer. A background
 * thread writes the buffer out once per second (and fsyncs under EVERYSEC);
 * under ALWAYS the caller writes and fsyncs before returning.
 *
 * Locking: file_mutex_ serializes every write to the file and every change of
 * fd_, so buffered chunks always reach the disk in the order they were
 * appended. buffer_mutex_ guards the buffers. Lock order: file -> buffer.
 */
class AofLogger {
public:
    explicit AofLogger(std::string filename = "miniredis.aof",
                       AofFsyncPolicy policy = AofFsyncPolicy::EVERYSEC);
    ~AofLogger();

    AofLogger(const AofLogger&) = delete;
    AofLogger& operator=(const AofLogger&) = delete;

    bool open();
    void close();
    [[nodiscard]] bool is_open() const noexcept { return open_.load(std::memory_order_acquire); }

    void append(const std::vector<std::string>& tokens);
    void append_raw(const std::string& raw_bytes);

    void set_policy(AofFsyncPolicy policy) noexcept { policy_.store(policy, std::memory_order_relaxed); }
    [[nodiscard]] AofFsyncPolicy policy() const noexcept { return policy_.load(std::memory_order_relaxed); }
    [[nodiscard]] const std::string& filename() const noexcept { return filename_; }

    // Write everything buffered so far and fsync it.
    void sync();

    // ---- BGREWRITEAOF support -------------------------------------------
    // Start copying every appended command into the rewrite delta. Must be
    // called at the same instant the snapshot is taken (with writes paused).
    void start_rewrite_delta();
    // Finish a rewrite: append the delta to temp_fd, fsync it, atomically
    // rename temp_path over target_path and, if target_path is this logger's
    // file, switch future appends to the new file. Appends are blocked only
    // for the final small step. Takes ownership of temp_fd.
    bool complete_rewrite(int temp_fd, const std::string& temp_path, const std::string& target_path);
    // Stop collecting the delta and discard it (rewrite failed).
    void abort_rewrite();
    [[nodiscard]] bool is_collecting_delta() const;

private:
    void background_flush_worker();
    // Requires file_mutex_. Writes out the primary buffer, then fsyncs if asked.
    void flush_locked(bool do_fsync);

    std::string filename_;
    std::atomic<AofFsyncPolicy> policy_;

    std::mutex file_mutex_;
    int fd_{-1};          // guarded by file_mutex_
    bool dirty_{false};   // written since last fsync; guarded by file_mutex_
    std::atomic<bool> open_{false};

    mutable std::mutex buffer_mutex_;
    std::condition_variable cv_;
    std::string primary_buffer_;
    bool collecting_delta_{false};
    std::string delta_buffer_;

    std::atomic<bool> running_{false};
    std::thread flusher_thread_;
};

} // namespace miniredis
