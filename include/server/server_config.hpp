#pragma once

#include "common/types.hpp"
#include <string>
#include <cstdint>

namespace miniredis {

struct ServerConfig {
    std::string host{DEFAULT_HOST};
    uint16_t port{DEFAULT_PORT};
    size_t worker_threads{4};
    size_t shard_count{DEFAULT_SHARD_COUNT};
    size_t max_memory{DEFAULT_MAX_MEMORY};
    EvictionPolicy eviction_policy{EvictionPolicy::ALLKEYS_LRU};
    
    // AOF persistence
    bool aof_enabled{true};
    std::string aof_filename{"miniredis.aof"};
    AofFsyncPolicy aof_fsync_policy{AofFsyncPolicy::EVERYSEC};

    // Logging
    std::string log_level{"INFO"};
};

} // namespace miniredis
