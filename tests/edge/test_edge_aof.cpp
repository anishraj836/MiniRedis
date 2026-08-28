#include "storage/storage_engine.hpp"
#include "aof/aof_logger.hpp"
#include "aof/aof_recovery.hpp"
#include "aof/aof_rewriter.hpp"
#include "server/command_registry.hpp"
#include <cassert>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>

using namespace miniredis;

// Edge Case 1: Truncated AOF File Recovery (Crash mid-write simulation)
void test_truncated_aof_crash_recovery() {
    const std::string trunc_aof = "test_truncated.aof";
    ::unlink(trunc_aof.c_str());

    CommandRegistry registry;

    // 1. Write 2 complete commands and 1 half-written truncated command
    {
        int fd = ::open(trunc_aof.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        assert(fd >= 0);

        // Command 1: SET k1 v1
        std::string cmd1 = "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n";
        // Command 2: SET k2 v2
        std::string cmd2 = "*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
        // Truncated Command 3 (Crash occurred mid-write)
        std::string cmd3_trunc = "*3\r\n$3\r\nSET\r\n$2\r\nk3\r\n$10\r\npartial";

        ssize_t w1 = ::write(fd, cmd1.data(), cmd1.size());
        ssize_t w2 = ::write(fd, cmd2.data(), cmd2.size());
        ssize_t w3 = ::write(fd, cmd3_trunc.data(), cmd3_trunc.size());
        (void)w1; (void)w2; (void)w3;
        ::close(fd);
    }

    // 2. Replay truncated file
    {
        StorageEngine engine(4);
        size_t replayed = 0;
        assert(AofRecovery::replay_file(trunc_aof, engine, registry, replayed));

        // Exactly 2 complete commands must be restored
        assert(replayed == 2);
        assert(engine.get_string("k1").value_or("") == "v1");
        assert(engine.get_string("k2").value_or("") == "v2");
        assert(!engine.exists("k3")); // Truncated command was safely ignored
    }

    ::unlink(trunc_aof.c_str());
    std::cout << "[PASS] test_truncated_aof_crash_recovery\n";
}

// Edge Case 2: Concurrent Background Rewrite under High Mutation Load
void test_concurrent_rewrite_under_load() {
    const std::string rewrite_aof = "test_concurrent_rewrite.aof";
    ::unlink(rewrite_aof.c_str());

    StorageEngine engine(8);
    CommandRegistry registry;

    // Seed initial dataset
    for (int i = 0; i < 500; ++i) {
        engine.set_string("init_k:" + std::to_string(i), "init_v:" + std::to_string(i));
    }

    AofLogger logger(rewrite_aof, AofFsyncPolicy::NO);
    assert(logger.open());

    // Trigger background rewrite
    assert(AofRewriter::start_background_rewrite(engine, logger, rewrite_aof));

    // Double trigger must fail gracefully
    assert(!AofRewriter::start_background_rewrite(engine, logger, rewrite_aof));

    // Mutate dataset heavily while rewrite runs
    for (int i = 0; i < 200; ++i) {
        std::string k = "live_k:" + std::to_string(i);
        std::string v = "live_v:" + std::to_string(i);
        engine.set_string(k, v);
        logger.append({"SET", k, v});
    }

    // Wait for rewrite to complete
    while (AofRewriter::is_rewrite_in_progress()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    logger.close();

    // Replay compacted AOF into clean engine
    StorageEngine restored_engine(8);
    size_t replayed = 0;
    assert(AofRecovery::replay_file(rewrite_aof, restored_engine, registry, replayed));

    // Verify all initial and live mutated keys exist
    for (int i = 0; i < 500; ++i) {
        assert(restored_engine.get_string("init_k:" + std::to_string(i)).has_value());
    }
    for (int i = 0; i < 200; ++i) {
        assert(restored_engine.get_string("live_k:" + std::to_string(i)).has_value());
    }

    ::unlink(rewrite_aof.c_str());
    std::cout << "[PASS] test_concurrent_rewrite_under_load (700 keys restored seamlessly)\n";
}

int main() {
    std::cout << "Running AOF Persistence Edge-Case Tests...\n";
    test_truncated_aof_crash_recovery();
    test_concurrent_rewrite_under_load();
    std::cout << "All AOF Persistence Edge-Case Tests Passed!\n";
    return 0;
}
