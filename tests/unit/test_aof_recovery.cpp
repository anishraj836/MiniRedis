#include "storage/storage_engine.hpp"
#include "aof/aof_logger.hpp"
#include "aof/aof_recovery.hpp"
#include "aof/aof_rewriter.hpp"
#include "server/command_registry.hpp"
#include <cassert>
#include <iostream>
#include <cstdio>
#include <unistd.h>

using namespace miniredis;

void test_aof_write_and_recovery() {
    const std::string test_aof = "test_persistence.aof";
    ::unlink(test_aof.c_str());

    CommandRegistry registry;

    // 1. Write commands to AOF file
    {
        AofLogger logger(test_aof, AofFsyncPolicy::ALWAYS);
        assert(logger.open());

        logger.append({"SET", "name", "Alice"});
        logger.append({"SET", "city", "San Francisco"});
        logger.append({"HSET", "user:1", "role", "admin", "status", "active"});
        logger.append({"RPUSH", "scores", "100", "200", "300"});
        logger.append({"DEL", "city"});
        logger.close();
    }

    // 2. Replay AOF file into fresh StorageEngine
    {
        StorageEngine restored_engine(8);
        size_t replayed = 0;
        assert(AofRecovery::replay_file(test_aof, restored_engine, registry, replayed));
        assert(replayed == 5);

        assert(restored_engine.get_string("name").value_or("") == "Alice");
        assert(!restored_engine.exists("city"));
        assert(restored_engine.get_shard("user:1").hget("user:1", "role").value_or("") == "admin");
        assert(restored_engine.get_shard("scores").llen("scores") == 3);
    }

    ::unlink(test_aof.c_str());
    std::cout << "[PASS] test_aof_write_and_recovery\n";
}

void test_aof_rewriter_compaction() {
    const std::string test_aof = "test_rewrite.aof";
    ::unlink(test_aof.c_str());

    StorageEngine engine(4);
    engine.set_string("k1", "v1");
    engine.get_shard("hash1").hset("hash1", {{"f1", "v1"}, {"f2", "v2"}});
    engine.get_shard("list1").rpush("list1", {"a", "b", "c"});

    AofLogger logger(test_aof, AofFsyncPolicy::ALWAYS);
    assert(logger.open());

    assert(AofRewriter::rewrite_synchronous(engine, logger, test_aof));
    logger.close();

    // Verify replayed state from compacted file
    StorageEngine restored_engine(4);
    CommandRegistry registry;
    size_t replayed = 0;
    assert(AofRecovery::replay_file(test_aof, restored_engine, registry, replayed));

    assert(restored_engine.get_string("k1").value_or("") == "v1");
    assert(restored_engine.get_shard("hash1").hlen("hash1") == 2);
    assert(restored_engine.get_shard("list1").llen("list1") == 3);

    ::unlink(test_aof.c_str());
    std::cout << "[PASS] test_aof_rewriter_compaction\n";
}

int main() {
    std::cout << "Running AOF Persistence Unit Tests...\n";
    test_aof_write_and_recovery();
    test_aof_rewriter_compaction();
    std::cout << "All AOF Persistence Unit Tests Passed!\n";
    return 0;
}
