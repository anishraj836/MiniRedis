#include "aof/aof_logger.hpp"
#include "protocol/resp_serializer.hpp"
#include "common/logger.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace miniredis {

AofLogger::AofLogger(std::string filename, AofFsyncPolicy policy)
    : filename_(std::move(filename)),
      policy_(policy) {}

AofLogger::~AofLogger() {
    close();
}

bool AofLogger::open() {
    fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        LOG_ERROR("Failed to open AOF file " + filename_ + ": " + strerror(errno));
        return false;
    }

    running_ = true;
    flusher_thread_ = std::thread(&AofLogger::background_flush_worker, this);
    LOG_INFO("AOF Persistence enabled. File: " + filename_ + ", Fsync: " + to_string(policy_));
    return true;
}

bool AofLogger::reopen(const std::string& new_filename) {
    std::string to_write;
    {
        std::unique_lock<std::mutex> lock(buffer_mutex_);
        if (!new_filename.empty()) {
            filename_ = new_filename;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ >= 0 && !primary_buffer_.empty()) {
            to_write.swap(primary_buffer_);
        }
    }
    if (!to_write.empty() && fd_ >= 0) {
        ssize_t written = ::write(fd_, to_write.data(), to_write.size());
        (void)written;
    }
    return fd_ >= 0;
}

void AofLogger::close() {
    if (running_.exchange(false)) {
        cv_.notify_all();
        if (flusher_thread_.joinable()) {
            flusher_thread_.join();
        }
    }

    if (fd_ >= 0) {
        sync();
        ::close(fd_);
        fd_ = -1;
    }
}

void AofLogger::append(const std::vector<std::string>& tokens) {
    if (tokens.empty() || fd_ < 0) return;
    std::string resp = RespSerializer::serialize_string_array(tokens);
    append_raw(resp);
}

void AofLogger::append_raw(const std::string& raw_bytes) {
    if (fd_ < 0) return;

    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        primary_buffer_.append(raw_bytes);
    }

    {
        std::lock_guard<std::mutex> lock(delta_mutex_);
        if (collecting_delta_) {
            delta_buffer_.append(raw_bytes);
        }
    }

    if (policy_ == AofFsyncPolicy::ALWAYS) {
        sync();
    }
}

void AofLogger::sync() {
    if (fd_ < 0) return;

    std::string to_write;
    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        to_write.swap(primary_buffer_);
    }

    if (!to_write.empty()) {
        ssize_t written = ::write(fd_, to_write.data(), to_write.size());
        (void)written;
    }

#if defined(__APPLE__)
    ::fcntl(fd_, F_FULLFSYNC);
#else
    ::fdatasync(fd_);
#endif
}

void AofLogger::background_flush_worker() {
    while (running_) {
        std::unique_lock<std::mutex> lock(buffer_mutex_);
        cv_.wait_for(lock, std::chrono::seconds(1), [this]() {
            return !running_ || !primary_buffer_.empty();
        });

        if (primary_buffer_.empty() && !running_) {
            break;
        }

        secondary_buffer_.clear();
        secondary_buffer_.swap(primary_buffer_);
        lock.unlock();

        if (!secondary_buffer_.empty() && fd_ >= 0) {
            ssize_t written = ::write(fd_, secondary_buffer_.data(), secondary_buffer_.size());
            (void)written;

            if (policy_ == AofFsyncPolicy::EVERYSEC) {
#if defined(__APPLE__)
                ::fcntl(fd_, F_FULLFSYNC);
#else
                ::fdatasync(fd_);
#endif
            }
        }
    }
}

void AofLogger::start_rewrite_delta() {
    std::lock_guard<std::mutex> lock(delta_mutex_);
    delta_buffer_.clear();
    collecting_delta_ = true;
}

std::string AofLogger::finish_rewrite_delta() {
    std::lock_guard<std::mutex> lock(delta_mutex_);
    collecting_delta_ = false;
    std::string res;
    res.swap(delta_buffer_);
    return res;
}

bool AofLogger::is_collecting_delta() const {
    return collecting_delta_;
}

} // namespace miniredis
