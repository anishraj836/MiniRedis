#pragma once

#include "storage/storage_engine.hpp"
#include "aof/aof_logger.hpp"
#include <string>
#include <atomic>
#include <thread>
#include <memory>

namespace miniredis {

class AofRewriter {
public:
    static bool rewrite_synchronous(StorageEngine& engine, AofLogger& logger, const std::string& target_filename);
    static bool start_background_rewrite(StorageEngine& engine, AofLogger& logger, const std::string& target_filename);
    static bool is_rewrite_in_progress() noexcept;

private:
    static std::atomic<bool> rewrite_in_progress_;
};

} // namespace miniredis
