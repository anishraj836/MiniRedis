#pragma once

#include "storage/storage_engine.hpp"
#include <string>
#include <cstdint>

namespace miniredis {

class CommandRegistry;

class AofRecovery {
public:
    static bool replay_file(const std::string& filename, StorageEngine& engine, CommandRegistry& registry, size_t& replayed_count);
};

} // namespace miniredis
