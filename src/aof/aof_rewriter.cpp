#include "aof/aof_rewriter.hpp"
#include "protocol/resp_serializer.hpp"
#include "common/io_utils.hpp"
#include "common/logger.hpp"
#include <condition_variable>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <optional>

namespace miniredis {

std::atomic<bool> AofRewriter::rewrite_in_progress_{false};

namespace {

std::mutex rewrite_mutex;          // one rewrite at a time (SAVE vs BGREWRITEAOF)
std::mutex background_mutex;
std::condition_variable background_cv;

void serialize_entry(const std::string& key, const ValueObject& val, std::string& out) {
    switch (val.type) {
        case RedisType::STRING:
            out += RespSerializer::serialize_string_array({"SET", key, val.as_string()});
            break;
        case RedisType::HASH: {
            const auto& hash = val.as_hash();
            if (hash.empty()) return;
            std::vector<std::string> args = {"HSET", key};
            args.reserve(2 + hash.size() * 2);
            for (const auto& [f, v] : hash) {
                args.push_back(f);
                args.push_back(v);
            }
            out += RespSerializer::serialize_string_array(args);
            break;
        }
        case RedisType::LIST: {
            const auto& list = val.as_list();
            if (list.empty()) return;
            std::vector<std::string> args = {"RPUSH", key};
            args.reserve(2 + list.size());
            args.insert(args.end(), list.begin(), list.end());
            out += RespSerializer::serialize_string_array(args);
            break;
        }
        case RedisType::SET: {
            const auto& set = val.as_set();
            if (set.empty()) return;
            std::vector<std::string> args = {"SADD", key};
            args.reserve(2 + set.size());
            args.insert(args.end(), set.begin(), set.end());
            out += RespSerializer::serialize_string_array(args);
            break;
        }
        case RedisType::NONE:
            return;
    }
    if (val.expire_at_ms >= 0) {
        out += RespSerializer::serialize_string_array({"PEXPIREAT", key, std::to_string(val.expire_at_ms)});
    }
}

} // anonymous namespace

bool AofRewriter::is_rewrite_in_progress() noexcept {
    return rewrite_in_progress_.load(std::memory_order_acquire);
}

bool AofRewriter::rewrite_synchronous(StorageEngine& engine, AofLogger& logger, const std::string& target_filename,
                                      KeyLockTable* write_locks) {
    std::unique_lock<std::mutex> one_at_a_time(rewrite_mutex, std::try_to_lock);
    if (!one_at_a_time.owns_lock()) {
        LOG_WARN("AOF rewrite requested while another rewrite is running");
        return false;
    }

    const std::string temp_file = target_filename + ".temp";
    int temp_fd = ::open(temp_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (temp_fd < 0) {
        LOG_ERROR("Failed to open temporary file for AOF rewrite: " + temp_file + ": " + strerror(errno));
        return false;
    }

    // Point-in-time snapshot. With all write locks held no command is half
    // way between "applied" and "appended", so starting the delta here splits
    // the command stream exactly at the snapshot.
    std::string snapshot;
    {
        std::optional<KeyLockTable::Guard> pause_writes;
        if (write_locks) pause_writes.emplace(write_locks->lock_all());

        logger.start_rewrite_delta();
        engine.snapshot_iterate([&snapshot](const std::string& key, const ValueObject& val) {
            serialize_entry(key, val, snapshot);
        });
    }

    if (!write_all(temp_fd, snapshot)) {
        LOG_ERROR("AOF rewrite failed writing snapshot to " + temp_file + ": " + strerror(errno));
        logger.abort_rewrite();
        ::close(temp_fd);
        ::unlink(temp_file.c_str());
        return false;
    }
    std::string().swap(snapshot); // free the snapshot memory before the final phase

    if (!logger.complete_rewrite(temp_fd, temp_file, target_filename)) {
        ::unlink(temp_file.c_str());
        return false;
    }

    LOG_INFO("AOF rewrite completed successfully. Compacted state saved to " + target_filename);
    return true;
}

bool AofRewriter::start_background_rewrite(StorageEngine& engine, AofLogger& logger, const std::string& target_filename,
                                           KeyLockTable* write_locks) {
    if (rewrite_in_progress_.exchange(true)) {
        LOG_WARN("Background AOF rewrite already in progress");
        return false;
    }

    std::thread([&engine, &logger, target_filename, write_locks]() {
        LOG_INFO("Background AOF rewrite started...");
        rewrite_synchronous(engine, logger, target_filename, write_locks);
        {
            std::lock_guard<std::mutex> lock(background_mutex);
            rewrite_in_progress_.store(false, std::memory_order_release);
        }
        background_cv.notify_all();
    }).detach();

    return true;
}

void AofRewriter::wait_for_background_rewrite() {
    std::unique_lock<std::mutex> lock(background_mutex);
    background_cv.wait(lock, [] { return !rewrite_in_progress_.load(std::memory_order_acquire); });
}

} // namespace miniredis
