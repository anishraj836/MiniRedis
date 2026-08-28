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

class AofLogger {
public:
    explicit AofLogger(std::string filename = "miniredis.aof",
                       AofFsyncPolicy policy = AofFsyncPolicy::EVERYSEC);
    ~AofLogger();

    AofLogger(const AofLogger&) = delete;
    AofLogger& operator=(const AofLogger&) = delete;

    bool open();
    bool reopen(const std::string& new_filename = "");
    void close();

    void append(const std::vector<std::string>& tokens);
    void append_raw(const std::string& raw_bytes);

    void set_policy(AofFsyncPolicy policy) noexcept { policy_ = policy; }
    [[nodiscard]] AofFsyncPolicy policy() const noexcept { return policy_; }
    [[nodiscard]] const std::string& filename() const noexcept { return filename_; }

    void sync();

    // Delta buffer management for BGREWRITEAOF
    void start_rewrite_delta();
    std::string finish_rewrite_delta();
    [[nodiscard]] bool is_collecting_delta() const;

private:
    void background_flush_worker();

    std::string filename_;
    AofFsyncPolicy policy_;
    int fd_{-1};

    std::mutex buffer_mutex_;
    std::condition_variable cv_;
    std::string primary_buffer_;
    std::string secondary_buffer_;

    std::atomic<bool> running_{false};
    std::thread flusher_thread_;

    // Delta buffer for BGREWRITEAOF
    std::mutex delta_mutex_;
    bool collecting_delta_{false};
    std::string delta_buffer_;
};

} // namespace miniredis
