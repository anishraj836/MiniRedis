#pragma once

#include "storage/storage_engine.hpp"
#include <string>
#include <cstdint>

namespace miniredis {

class CommandRegistry;

class AofRecovery {
public:
    // Replays every complete command in `filename` into `engine`.
    //  - Missing file: fresh start, returns true.
    //  - File ends in a partially written command (crash mid-append): the
    //    partial tail is truncated away so later appends are not stuck behind
    //    garbage, and true is returned.
    //  - Corruption in the middle of the file: returns false. Loading only part
    //    of the history would silently serve wrong data.
    static bool replay_file(const std::string& filename, StorageEngine& engine, CommandRegistry& registry, size_t& replayed_count);
};

} // namespace miniredis
