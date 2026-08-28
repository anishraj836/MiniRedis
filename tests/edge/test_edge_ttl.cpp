#include "storage/storage_engine.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>

using namespace miniredis;

// Edge Case 1: Negative or Zero Expiry Values (Immediate Expiration)
void test_immediate_and_negative_expiration() {
    StorageEngine engine(4);

    engine.set_string("k_zero_ttl", "val");
    assert(engine.exists("k_zero_ttl"));

    // Setting expire with 0 or negative ms -> key must expire immediately
    assert(engine.expire("k_zero_ttl", now_ms() - 1000));
    assert(!engine.exists("k_zero_ttl"));
    assert(!engine.get_string("k_zero_ttl").has_value());
    assert(engine.ttl_ms("k_zero_ttl") == -2);

    std::cout << "[PASS] test_immediate_and_negative_expiration\n";
}

// Edge Case 2: PERSIST Lifecycle and Min-Heap Entry Invalidation
void test_persist_lifecycle() {
    StorageEngine engine(4);

    // 1. PERSIST on non-existent key -> false
    assert(!engine.persist("no_such_key"));

    // 2. PERSIST on key without TTL -> false (already persistent)
    engine.set_string("perm_key", "perm_val");
    assert(!engine.persist("perm_key"));

    // 3. PERSIST on key WITH TTL -> true
    engine.set_string("temp_key", "temp_val", now_ms() + 50000);
    assert(engine.ttl_ms("temp_key") > 0);
    assert(engine.persist("temp_key"));
    assert(engine.ttl_ms("temp_key") == -1); // -1 = persistent

    // Verify key still exists and value is intact
    assert(engine.get_string("temp_key").value_or("") == "temp_val");

    std::cout << "[PASS] test_persist_lifecycle\n";
}

// Edge Case 3: TTL Updates and Rescheduling
void test_ttl_rescheduling() {
    StorageEngine engine(4);

    // Initial expiration at +500ms
    engine.set_string("k_resched", "val", now_ms() + 500);
    assert(engine.ttl_ms("k_resched") > 0);

    // Reschedule expiration further into future (+50000ms)
    assert(engine.expire("k_resched", now_ms() + 50000));
    assert(engine.ttl_ms("k_resched") > 10000);

    // Reschedule to short expiry (+30ms)
    assert(engine.expire("k_resched", now_ms() + 30));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Must be expired now
    assert(!engine.exists("k_resched"));
    assert(engine.ttl_ms("k_resched") == -2);

    std::cout << "[PASS] test_ttl_rescheduling\n";
}

// Edge Case 4: Passive Expiry on Complex Types (Hash, List, Set)
void test_passive_expiry_complex_types() {
    StorageEngine engine(4);
    int64_t expire_at = now_ms() + 40;

    auto& s1 = engine.get_shard("exp_hash");
    s1.hset("exp_hash", {{"field1", "val1"}});
    engine.expire("exp_hash", expire_at);

    auto& s2 = engine.get_shard("exp_list");
    s2.rpush("exp_list", {"elem1", "elem2"});
    engine.expire("exp_list", expire_at);

    auto& s3 = engine.get_shard("exp_set");
    s3.sadd("exp_set", {"member1"});
    engine.expire("exp_set", expire_at);

    // Wait past expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Accessing sub-methods must trigger passive eviction
    assert(!s1.hget("exp_hash", "field1").has_value());
    assert(s1.hlen("exp_hash") == 0);

    assert(!s2.lpop("exp_list").has_value());
    assert(s2.llen("exp_list") == 0);

    assert(!s3.sismember("exp_set", "member1"));
    assert(s3.scard("exp_set") == 0);

    std::cout << "[PASS] test_passive_expiry_complex_types\n";
}

int main() {
    std::cout << "Running TTL & Expiration Edge-Case Tests...\n";
    test_immediate_and_negative_expiration();
    test_persist_lifecycle();
    test_ttl_rescheduling();
    test_passive_expiry_complex_types();
    std::cout << "All TTL & Expiration Edge-Case Tests Passed!\n";
    return 0;
}
