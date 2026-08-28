#include "aof/aof_rewriter.hpp"
#include "protocol/resp_serializer.hpp"
#include "common/logger.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <cstdio>

namespace miniredis {

std::atomic<bool> AofRewriter::rewrite_in_progress_{false};

bool AofRewriter::is_rewrite_in_progress() noexcept {
    return rewrite_in_progress_.load(std::memory_order_relaxed);
}

bool AofRewriter::rewrite_synchronous(StorageEngine& engine, AofLogger& logger, const std::string& target_filename) {
    std::string temp_file = target_filename + ".temp";
    int temp_fd = ::open(temp_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (temp_fd < 0) {
        LOG_ERROR("Failed to open temporary file for AOF rewrite: " + temp_file + ": " + strerror(errno));
        return false;
    }

    if (!logger.is_collecting_delta()) {
        logger.start_rewrite_delta();
    }

    // Iterate through all key-values across all shards
    engine.snapshot_iterate([temp_fd](const std::string& key, const ValueObject& val) {
        std::string command_payload;
        switch (val.type) {
            case RedisType::STRING: {
                command_payload = RespSerializer::serialize_string_array({"SET", key, val.as_string()});
                if (val.expire_at_ms >= 0) {
                    command_payload += RespSerializer::serialize_string_array({"PEXPIREAT", key, std::to_string(val.expire_at_ms)});
                }
                break;
            }
            case RedisType::HASH: {
                const auto& hash = val.as_hash();
                if (!hash.empty()) {
                    std::vector<std::string> args = {"HSET", key};
                    for (const auto& [f, v] : hash) {
                        args.push_back(f);
                        args.push_back(v);
                    }
                    command_payload = RespSerializer::serialize_string_array(args);
                }
                if (val.expire_at_ms >= 0) {
                    command_payload += RespSerializer::serialize_string_array({"PEXPIREAT", key, std::to_string(val.expire_at_ms)});
                }
                break;
            }
            case RedisType::LIST: {
                const auto& list = val.as_list();
                if (!list.empty()) {
                    std::vector<std::string> args = {"RPUSH", key};
                    for (const auto& item : list) {
                        args.push_back(item);
                    }
                    command_payload = RespSerializer::serialize_string_array(args);
                }
                if (val.expire_at_ms >= 0) {
                    command_payload += RespSerializer::serialize_string_array({"PEXPIREAT", key, std::to_string(val.expire_at_ms)});
                }
                break;
            }
            case RedisType::SET: {
                const auto& set = val.as_set();
                if (!set.empty()) {
                    std::vector<std::string> args = {"SADD", key};
                    for (const auto& item : set) {
                        args.push_back(item);
                    }
                    command_payload = RespSerializer::serialize_string_array(args);
                }
                if (val.expire_at_ms >= 0) {
                    command_payload += RespSerializer::serialize_string_array({"PEXPIREAT", key, std::to_string(val.expire_at_ms)});
                }
                break;
            }
            case RedisType::NONE:
                break;
        }

        if (!command_payload.empty()) {
            ssize_t w = ::write(temp_fd, command_payload.data(), command_payload.size());
            (void)w;
        }
    });

    // Append accumulated delta modifications
    std::string delta = logger.finish_rewrite_delta();
    if (!delta.empty()) {
        ssize_t w = ::write(temp_fd, delta.data(), delta.size());
        (void)w;
    }

#if defined(__APPLE__)
    ::fcntl(temp_fd, F_FULLFSYNC);
#else
    ::fdatasync(temp_fd);
#endif
    ::close(temp_fd);

    // Atomically replace target AOF file
    if (::rename(temp_file.c_str(), target_filename.c_str()) < 0) {
        LOG_ERROR("Failed to rename temporary AOF rewrite file: " + std::string(strerror(errno)));
        return false;
    }

    // Reopen logger to newly replaced file descriptor
    logger.reopen(target_filename);

    LOG_INFO("AOF rewrite completed successfully. Compacted state saved to " + target_filename);
    return true;
}

bool AofRewriter::start_background_rewrite(StorageEngine& engine, AofLogger& logger, const std::string& target_filename) {
    if (rewrite_in_progress_.exchange(true)) {
        LOG_WARN("Background AOF rewrite already in progress");
        return false;
    }

    // Synchronously activate delta collection BEFORE launching the background thread
    logger.start_rewrite_delta();

    std::thread([&engine, &logger, target_filename]() {
        LOG_INFO("Background AOF rewrite started...");
        rewrite_synchronous(engine, logger, target_filename);
        rewrite_in_progress_.store(false, std::memory_order_release);
    }).detach();

    return true;
}

} // namespace miniredis
