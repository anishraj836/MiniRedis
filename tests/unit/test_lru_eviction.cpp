#include "storage/storage_engine.hpp"
#include <cassert>
#include <iostream>

using namespace miniredis;

void test_lru_eviction_single_shard() {
    // 1 shard with 650 bytes limit (space for exactly 3 string entries ~180 bytes each)
    StorageEngine engine(1, 650, EvictionPolicy::ALLKEYS_LRU);

    assert(engine.set_string("k1", "val1"));
    assert(engine.set_string("k2", "val2"));
    assert(engine.set_string("k3", "val3"));

    // All 3 keys currently present
    assert(engine.exists("k1"));
    assert(engine.exists("k2"));
    assert(engine.exists("k3"));

    // Access k1 so it is promoted to MRU, leaving k2 as the Least Recently Used
    assert(engine.get_string("k1").has_value());

    // Insert k4, which exceeds 650 bytes and triggers LRU eviction of k2
    assert(engine.set_string("k4", "val4"));

    assert(engine.get_string("k1").has_value());  // k1 preserved (MRU)
    assert(!engine.get_string("k2").has_value()); // k2 evicted (LRU)
    assert(engine.get_string("k3").has_value());  // k3 preserved
    assert(engine.get_string("k4").has_value());  // k4 newly added
    std::cout << "[PASS] test_lru_eviction_single_shard\n";
}

void test_noeviction_oom() {
    StorageEngine engine(1, 200, EvictionPolicy::NO_EVICTION);

    assert(engine.set_string("k1", "val1"));
    // Further insert will fail with OOM
    bool ok = engine.set_string("k2", std::string(1000, 'X'));
    assert(!ok);
    std::cout << "[PASS] test_noeviction_oom\n";
}

int main() {
    std::cout << "Running LRU Eviction Unit Tests...\n";
    test_lru_eviction_single_shard();
    test_noeviction_oom();
    std::cout << "All LRU Eviction Unit Tests Passed!\n";
    return 0;
}
