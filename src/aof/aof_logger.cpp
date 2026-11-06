#include "aof/aof_logger.hpp"
#include "protocol/resp_serializer.hpp"
#include "common/io_utils.hpp"
#include "common/logger.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstring>

namespace miniredis {

namespace {
constexpr size_t REWRITE_DRAIN_THRESHOLD = 64 * 1024;
}

AofLogger::AofLogger(std::string filename, AofFsyncPolicy policy)
    : filename_(std::move(filename)),
      policy_(policy) {}

AofLogger::~AofLogger() {
    close();
}

bool AofLogger::open() {
    std::lock_guard<std::mutex> file_lock(file_mutex_);
    if (open_.load()) {
        return true; // a second open() used to start a second flusher thread
    }

    fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        LOG_ERROR("Failed to open AOF file " + filename_ + ": " + strerror(errno));
        return false;
    }

    open_.store(true, std::memory_order_release);
    running_ = true;
    flusher_thread_ = std::thread(&AofLogger::background_flush_worker, this);
    LOG_INFO("AOF Persistence enabled. File: " + filename_ + ", Fsync: " + to_string(policy()));
    return true;
}

void AofLogger::close() {
    if (running_.exchange(false)) {
        cv_.notify_all();
        if (flusher_thread_.joinable()) {
            flusher_thread_.join();
        }
    }

    std::lock_guard<std::mutex> file_lock(file_mutex_);
    open_.store(false, std::memory_order_release);
    if (fd_ >= 0) {
        flush_locked(true);
        ::close(fd_);
        fd_ = -1;
    }
}

void AofLogger::append(const std::vector<std::string>& tokens) {
    if (tokens.empty() || !is_open()) return;
    append_raw(RespSerializer::serialize_string_array(tokens));
}

void AofLogger::append_raw(const std::string& raw_bytes) {
    if (!is_open()) return;

    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        primary_buffer_.append(raw_bytes);
        if (collecting_delta_) {
            delta_buffer_.append(raw_bytes);
        }
    }

    if (policy() == AofFsyncPolicy::ALWAYS) {
        sync();
    }
}

void AofLogger::flush_locked(bool do_fsync) {
    std::string to_write;
    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        to_write.swap(primary_buffer_);
    }

    if (fd_ < 0) return;

    if (!to_write.empty()) {
        if (!write_all(fd_, to_write)) {
            LOG_ERROR("AOF write failed (" + std::to_string(to_write.size()) + " bytes lost): " + strerror(errno));
        }
        dirty_ = true;
    }

    if (do_fsync && dirty_) {
        if (!fsync_fd(fd_)) {
            LOG_ERROR("AOF fsync failed: " + std::string(strerror(errno)));
        }
        dirty_ = false;
    }
}

void AofLogger::sync() {
    std::lock_guard<std::mutex> file_lock(file_mutex_);
    flush_locked(true);
}

void AofLogger::background_flush_worker() {
    while (running_) {
        {
            std::unique_lock<std::mutex> lock(buffer_mutex_);
            cv_.wait_for(lock, std::chrono::seconds(1), [this]() { return !running_; });
        }
        if (!running_) break; // close() does the final flush

        std::lock_guard<std::mutex> file_lock(file_mutex_);
        flush_locked(policy() == AofFsyncPolicy::EVERYSEC);
    }
}

void AofLogger::start_rewrite_delta() {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    delta_buffer_.clear();
    collecting_delta_ = true;
}

void AofLogger::abort_rewrite() {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    collecting_delta_ = false;
    delta_buffer_.clear();
    delta_buffer_.shrink_to_fit();
}

bool AofLogger::is_collecting_delta() const {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    return collecting_delta_;
}

bool AofLogger::complete_rewrite(int temp_fd, const std::string& temp_path, const std::string& target_path) {
    auto fail = [&](const std::string& what) {
        LOG_ERROR("AOF rewrite failed: " + what + ": " + strerror(errno));
        abort_rewrite();
        ::close(temp_fd);
        return false;
    };

    // Phase 1: move most of the delta into the temp file while writers keep
    // running, so the final pause below only has a small tail to copy.
    for (int round = 0; round < 16; ++round) {
        std::string chunk;
        {
            std::lock_guard<std::mutex> lock(buffer_mutex_);
            if (delta_buffer_.size() < REWRITE_DRAIN_THRESHOLD) break;
            chunk.swap(delta_buffer_);
        }
        if (!write_all(temp_fd, chunk)) return fail("writing delta to " + temp_path);
    }
    if (!fsync_fd(temp_fd)) return fail("fsync of " + temp_path);

    // Phase 2: block appends and file writes, then swap atomically.
    std::lock_guard<std::mutex> file_lock(file_mutex_);
    std::unique_lock<std::mutex> buffer_lock(buffer_mutex_);

    std::string tail;
    tail.swap(delta_buffer_);
    collecting_delta_ = false;

    if (!write_all(temp_fd, tail) || !fsync_fd(temp_fd)) {
        buffer_lock.unlock();
        return fail("writing final delta to " + temp_path);
    }
    ::close(temp_fd);

    // Whatever is still buffered for the current file was appended either
    // before the snapshot (its effect is in the snapshot) or after it (it is
    // also in the delta we just wrote). Either way it belongs to the old file:
    // write it there, so the old file stays complete if the rename fails.
    // (The old code instead copied these bytes into the *new* file, applying
    // them twice on replay, and flushed later appends into the replaced file,
    // losing them.)
    std::string pending;
    pending.swap(primary_buffer_);
    if (fd_ >= 0 && !pending.empty()) {
        write_all(fd_, pending);
        dirty_ = true;
    }

    if (::rename(temp_path.c_str(), target_path.c_str()) < 0) {
        LOG_ERROR("AOF rewrite failed: rename " + temp_path + " -> " + target_path + ": " + strerror(errno));
        ::unlink(temp_path.c_str());
        return false;
    }
    fsync_parent_dir(target_path);

    if (fd_ >= 0 && target_path == filename_) {
        int new_fd = ::open(target_path.c_str(), O_WRONLY | O_APPEND);
        if (new_fd < 0) {
            LOG_ERROR("AOF rewrite: could not reopen " + target_path + ": " + strerror(errno));
            return false;
        }
        ::close(fd_);
        fd_ = new_fd;
        dirty_ = false;
    }
    return true;
}

} // namespace miniredis
