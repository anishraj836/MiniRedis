#include "storage/storage_engine.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>

using namespace miniredis;

void test_strings() {
    StorageEngine engine(16);
    assert(engine.set_string("foo", "bar"));
    assert(engine.exists("foo"));

    auto val = engine.get_string("foo");
    assert(val.has_value());
    assert(val.value() == "bar");

    assert(engine.del("foo"));
    assert(!engine.exists("foo"));
    assert(!engine.get_string("foo").has_value());
    std::cout << "[PASS] test_strings\n";
}

void test_hashes() {
    StorageEngine engine(16);
    auto& shard = engine.get_shard("user:1000");

    assert(shard.hset("user:1000", {{"name", "Alice"}, {"email", "alice@example.com"}}) == 2);
    assert(shard.hexists("user:1000", "name"));
    assert(shard.hlen("user:1000") == 2);

    auto name = shard.hget("user:1000", "name");
    assert(name.has_value() && name.value() == "Alice");

    auto all = shard.hgetall("user:1000");
    assert(all.has_value() && all.value().size() == 2);

    assert(shard.hdel("user:1000", "email"));
    assert(shard.hlen("user:1000") == 1);
    std::cout << "[PASS] test_hashes\n";
}

void test_lists() {
    StorageEngine engine(16);
    auto& shard = engine.get_shard("tasks");

    assert(shard.rpush("tasks", {"t1", "t2", "t3"}) == 3);
    assert(shard.lpush("tasks", {"t0"}) == 4);
    assert(shard.llen("tasks") == 4);

    auto range = shard.lrange("tasks", 0, -1);
    assert(range.size() == 4);
    assert(range[0] == "t0" && range[1] == "t1" && range[2] == "t2" && range[3] == "t3");

    auto pop1 = shard.lpop("tasks");
    assert(pop1.has_value() && pop1.value() == "t0");

    auto pop2 = shard.rpop("tasks");
    assert(pop2.has_value() && pop2.value() == "t3");
    assert(shard.llen("tasks") == 2);
    std::cout << "[PASS] test_lists\n";
}

void test_sets() {
    StorageEngine engine(16);
    auto& shard = engine.get_shard("fruits");

    assert(shard.sadd("fruits", {"apple", "banana", "cherry", "apple"}) == 3);
    assert(shard.scard("fruits") == 3);
    assert(shard.sismember("fruits", "banana"));
    assert(!shard.sismember("fruits", "orange"));

    assert(shard.srem("fruits", {"banana"}) == 1);
    assert(shard.scard("fruits") == 2);
    std::cout << "[PASS] test_sets\n";
}

void test_concurrent_access() {
    StorageEngine engine(32);
    constexpr size_t num_threads = 8;
    constexpr size_t ops_per_thread = 5000;

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&engine, t]() {
            for (size_t i = 0; i < ops_per_thread; ++i) {
                std::string k = "key:" + std::to_string(t) + ":" + std::to_string(i);
                engine.set_string(k, "val");
                auto v = engine.get_string(k);
                assert(v.has_value() && v.value() == "val");
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(engine.total_keys() == num_threads * ops_per_thread);
    std::cout << "[PASS] test_concurrent_access (" << num_threads * ops_per_thread << " concurrent writes/reads)\n";
}

int main() {
    std::cout << "Running StorageEngine Unit Tests...\n";
    test_strings();
    test_hashes();
    test_lists();
    test_sets();
    test_concurrent_access();
    std::cout << "All StorageEngine Unit Tests Passed!\n";
    return 0;
}
