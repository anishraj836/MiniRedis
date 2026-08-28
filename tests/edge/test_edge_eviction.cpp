#include "storage/storage_engine.hpp"
#include <cassert>
#include <iostream>
#include <string>

using namespace miniredis;

// Edge Case 1: Key Overwrite Memory Accounting (Shrinking & Growing In-Place)
void test_key_overwrite_memory_accounting() {
    StorageEngine engine(1, 100000, EvictionPolicy::ALLKEYS_LRU);

    size_t initial_mem = engine.total_memory_bytes();
    assert(initial_mem == 0);

    // 1. Insert large string (10KB)
    std::string large_val(10000, 'A');
    assert(engine.set_string("k_resize", large_val));
    size_t mem_after_large = engine.total_memory_bytes();
    assert(mem_after_large >= 10000);

    // 2. Overwrite with small string (100B) -> memory must decrease, not leak!
    std::string small_val(100, 'B');
    assert(engine.set_string("k_resize", small_val));
    size_t mem_after_small = engine.total_memory_bytes();
    assert(mem_after_small < mem_after_large);
    assert(mem_after_small < 1000);

    // 3. Delete key -> memory must return to 0 (no underflow)
    assert(engine.del("k_resize"));
    assert(engine.total_memory_bytes() == 0);

    std::cout << "[PASS] test_key_overwrite_memory_accounting\n";
}

// Edge Case 2: Hot-Key Preservation under Heavy Eviction
void test_hot_key_preservation() {
    // Shard with limit for ~10 keys (~2000 bytes)
    StorageEngine engine(1, 2000, EvictionPolicy::ALLKEYS_LRU);

    // Insert hot key
    assert(engine.set_string("HOT_KEY", "hot_value"));

    // Insert 500 cold keys, touching HOT_KEY after every 2 insertions
    for (int i = 0; i < 500; ++i) {
        engine.set_string("cold_" + std::to_string(i), "val_" + std::to_string(i));
        if (i % 2 == 0) {
            auto hot = engine.get_string("HOT_KEY");
            assert(hot.has_value());
        }
    }

    // Hot key must STILL exist because it was continually refreshed to MRU
    auto final_hot = engine.get_string("HOT_KEY");
    assert(final_hot.has_value() && final_hot.value() == "hot_value");

    std::cout << "[PASS] test_hot_key_preservation (500 cold key evictions survived)\n";
}

// Edge Case 3: VOLATILE_LRU vs ALLKEYS_LRU Policy Invariants
void test_volatile_vs_allkeys_lru() {
    // 1. Under VOLATILE_LRU: keys without expiration are NEVER evicted
    {
        StorageEngine engine(1, 1000, EvictionPolicy::VOLATILE_LRU);

        // Insert persistent key (no TTL)
        assert(engine.set_string("persistent_key", "permanent_val"));

        // Insert volatile key (with TTL)
        assert(engine.set_string("volatile_key", "temp_val", now_ms() + 100000));

        // Insert more volatile keys to force eviction
        for (int i = 0; i < 20; ++i) {
            engine.set_string("vol_" + std::to_string(i), "temp", now_ms() + 100000);
        }

        // Persistent key MUST remain because VOLATILE_LRU only evicts volatile keys
        assert(engine.exists("persistent_key"));
    }

    // 2. Under ALLKEYS_LRU: all keys are eligible for eviction
    {
        StorageEngine engine(1, 500, EvictionPolicy::ALLKEYS_LRU);

        assert(engine.set_string("unprotected_key", "val"));

        // Insert enough keys to evict unprotected_key
        for (int i = 0; i < 15; ++i) {
            engine.set_string("k_" + std::to_string(i), "v");
        }

        // Unprotected key should be evicted
        assert(!engine.exists("unprotected_key"));
    }

    std::cout << "[PASS] test_volatile_vs_allkeys_lru\n";
}

int main() {
    std::cout << "Running LRU & Memory Edge-Case Tests...\n";
    test_key_overwrite_memory_accounting();
    test_hot_key_preservation();
    test_volatile_vs_allkeys_lru();
    std::cout << "All LRU & Memory Edge-Case Tests Passed!\n";
    return 0;
}
