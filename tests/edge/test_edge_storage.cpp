#include "storage/storage_engine.hpp"
#include <cassert>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

using namespace miniredis;

// Edge Case 1: Type collision / WRONGTYPE operations
void test_type_collision() {
    StorageEngine engine(8);

    // Set key as STRING
    assert(engine.set_string("coll_key", "string_val"));
    assert(engine.type("coll_key") == RedisType::STRING);

    auto& shard = engine.get_shard("coll_key");

    // Attempt HSET on a STRING -> must fail with WRONGTYPE
    int64_t hres = shard.hset("coll_key", {{"f1", "v1"}});
    assert(hres == StorageShard::WRONG_TYPE);

    // Attempt LPUSH on a STRING -> WRONGTYPE (it used to return 0, which the
    // client saw as "(integer) 0" instead of an error), string unmodified
    int64_t lres = shard.lpush("coll_key", {"item1"});
    assert(lres == StorageShard::WRONG_TYPE);

    // Attempt SADD on a STRING -> WRONGTYPE, string unmodified
    int64_t sres = shard.sadd("coll_key", {"item1"});
    assert(sres == StorageShard::WRONG_TYPE);

    // Verify string was not corrupted
    assert(engine.get_string("coll_key").value_or("") == "string_val");

    std::cout << "[PASS] test_type_collision\n";
}

// Edge Case 2: LRANGE boundary & negative indexing edge cases
void test_lrange_boundaries() {
    StorageEngine engine(4);
    auto& shard = engine.get_shard("lrange_list");

    // Elements: ["e0", "e1", "e2", "e3", "e4"] (indices 0..4, or -5..-1)
    shard.rpush("lrange_list", {"e0", "e1", "e2", "e3", "e4"});
    assert(shard.llen("lrange_list") == 5);

    // 1. Full list: LRANGE 0 -1
    auto r1 = shard.lrange("lrange_list", 0, -1);
    assert(r1.size() == 5);
    assert(r1[0] == "e0" && r1[4] == "e4");

    // 2. Out of range positive: LRANGE 2 100 -> should return ["e2", "e3", "e4"]
    auto r2 = shard.lrange("lrange_list", 2, 100);
    assert(r2.size() == 3);
    assert(r2[0] == "e2" && r2[2] == "e4");

    // 3. Start > stop: LRANGE 4 2 -> empty list
    auto r3 = shard.lrange("lrange_list", 4, 2);
    assert(r3.empty());

    // 4. Large negative index: LRANGE -100 2 -> should clamp start to 0 -> ["e0", "e1", "e2"]
    auto r4 = shard.lrange("lrange_list", -100, 2);
    assert(r4.size() == 3);
    assert(r4[0] == "e0" && r4[2] == "e2");

    // 5. LRANGE on non-existent list -> empty list
    auto r5 = shard.lrange("non_existent", 0, -1);
    assert(r5.empty());

    std::cout << "[PASS] test_lrange_boundaries\n";
}

// Edge Case 3: Empty keys, UTF-8 keys, and binary safety
void test_empty_and_binary_keys() {
    StorageEngine engine(16);

    // 1. Empty string key ""
    assert(engine.set_string("", "empty_key_val"));
    assert(engine.exists(""));
    assert(engine.get_string("").value_or("") == "empty_key_val");
    assert(engine.del(""));
    assert(!engine.exists(""));

    // 2. Binary key with null bytes "\0\1\2\0"
    std::string bin_key("k\0ey\0bin", 8);
    std::string bin_val("v\0al\0bin", 8);
    assert(engine.set_string(bin_key, bin_val));
    assert(engine.exists(bin_key));
    assert(engine.get_string(bin_key).value_or("") == bin_val);

    // 3. UTF-8 multi-byte keys (e.g. Emoji / Chinese characters)
    std::string utf8_key = "用户:🚀:1000";
    std::string utf8_val = "数据:🎉:SUCCESS";
    assert(engine.set_string(utf8_key, utf8_val));
    assert(engine.get_string(utf8_key).value_or("") == utf8_val);

    std::cout << "[PASS] test_empty_and_binary_keys\n";
}

// Edge Case 4: Non-existent elements & popping to empty
void test_empty_collection_operations() {
    StorageEngine engine(4);
    auto& shard = engine.get_shard("coll");

    // Pop from non-existent list
    assert(!shard.lpop("no_list").has_value());
    assert(!shard.rpop("no_list").has_value());

    // Push 1 item and pop it
    shard.rpush("single_item_list", {"item1"});
    assert(shard.lpop("single_item_list").value_or("") == "item1");
    // Next pop on now empty list returns nullopt
    assert(!shard.lpop("single_item_list").has_value());

    // HDEL on non-existent hash or field
    assert(!shard.hdel("no_hash", "f1"));
    shard.hset("h1", {{"f1", "v1"}});
    assert(!shard.hdel("h1", "f2")); // non-existent field
    assert(shard.hdel("h1", "f1"));  // existing field
    assert(!shard.hdel("h1", "f1")); // already deleted

    // SREM on non-existent set or member
    assert(shard.srem("no_set", {"m1"}) == 0);
    shard.sadd("s1", {"m1"});
    assert(shard.srem("s1", {"m2"}) == 0); // non-existent member
    assert(shard.srem("s1", {"m1"}) == 1); // existing member
    assert(shard.scard("s1") == 0);

    std::cout << "[PASS] test_empty_collection_operations\n";
}

// Edge Case 5: Deadlock-free concurrent MSET across 16 threads
void test_concurrent_mset_deadlock_freedom() {
    StorageEngine engine(32);
    constexpr size_t num_threads = 16;
    constexpr size_t iterations = 500;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&engine, t]() {
            for (size_t i = 0; i < iterations; ++i) {
                // Cross-shard key pairs in varying order
                std::vector<std::pair<std::string, std::string>> kvs = {
                    {"key_A:" + std::to_string((t + i) % 100), "valA"},
                    {"key_Z:" + std::to_string((t * 3 + i) % 100), "valZ"},
                    {"key_M:" + std::to_string((t * 7 + i) % 100), "valM"}
                };
                assert(engine.mset(kvs));
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "[PASS] test_concurrent_mset_deadlock_freedom (" << num_threads * iterations << " multi-key ops)\n";
}

int main() {
    std::cout << "Running Storage Edge-Case Tests...\n";
    test_type_collision();
    test_lrange_boundaries();
    test_empty_and_binary_keys();
    test_empty_collection_operations();
    test_concurrent_mset_deadlock_freedom();
    std::cout << "All Storage Edge-Case Tests Passed!\n";
    return 0;
}
