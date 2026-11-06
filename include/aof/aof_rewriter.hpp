#pragma once

#include "storage/storage_engine.hpp"
#include "aof/aof_logger.hpp"
#include "common/key_lock_table.hpp"
#include <string>
#include <atomic>
#include <thread>
#include <memory>

namespace miniredis {

class AofRewriter {
public:
    // Compacts the current dataset into target_filename.
    //
    // `write_locks`, when given, are the server's key write locks: the snapshot
    // is taken while all of them are held, so every write command is either
    // fully reflected in the snapshot or fully contained in the delta that is
    // appended afterwards, never both and never neither. Only the in-memory
    // serialization runs under the locks; file I/O happens after they are
    // released. Reads are never blocked.
    //
    // Returns false if another rewrite is running or on I/O failure.
    static bool rewrite_synchronous(StorageEngine& engine, AofLogger& logger, const std::string& target_filename,
                                    KeyLockTable* write_locks = nullptr);
    static bool start_background_rewrite(StorageEngine& engine, AofLogger& logger, const std::string& target_filename,
                                         KeyLockTable* write_locks = nullptr);
    static bool is_rewrite_in_progress() noexcept;
    // Blocks until a background rewrite (if any) has finished. Call before
    // destroying the engine or logger it uses.
    static void wait_for_background_rewrite();

private:
    static std::atomic<bool> rewrite_in_progress_;
};

} // namespace miniredis
