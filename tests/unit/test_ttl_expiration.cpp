#include "storage/storage_engine.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>

using namespace miniredis;

void test_passive_expiration() {
    StorageEngine engine(4);
    int64_t expire_at = now_ms() + 50; // 50ms TTL

    engine.set_string("temp_key", "temp_value", expire_at);
    assert(engine.exists("temp_key"));
    assert(engine.get_string("temp_key").has_value());

    // Sleep for expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(70));

    // Passive expiration on access
    assert(!engine.exists("temp_key"));
    assert(!engine.get_string("temp_key").has_value());
    assert(engine.ttl_ms("temp_key") == -2);
    std::cout << "[PASS] test_passive_expiration\n";
}

void test_active_expiration_cycle() {
    StorageEngine engine(4);
    int64_t expire_at = now_ms() + 30;

    for (int i = 0; i < 50; ++i) {
        engine.set_string("exp:" + std::to_string(i), "val", expire_at);
    }
    for (int i = 0; i < 50; ++i) {
        engine.set_string("persist:" + std::to_string(i), "val");
    }

    assert(engine.total_keys() == 100);

    // Sleep past expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Run active expire cycle
    size_t swept = engine.active_expire_cycle();
    assert(swept == 50);
    assert(engine.total_keys() == 50);
    std::cout << "[PASS] test_active_expiration_cycle (swept 50 keys)\n";
}

int main() {
    std::cout << "Running TTL Expiration Unit Tests...\n";
    test_passive_expiration();
    test_active_expiration_cycle();
    std::cout << "All TTL Expiration Unit Tests Passed!\n";
    return 0;
}
