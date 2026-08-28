#include "server/redis_server.hpp"
#include "client/redis_client.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

using namespace miniredis;

void test_concurrent_tcp_load() {
    ServerConfig config;
    config.host = "127.0.0.1";
    config.port = 16380;
    config.worker_threads = 4;
    config.shard_count = 16;
    config.aof_enabled = false;

    auto server = std::make_unique<RedisServer>(config);
    std::thread server_thread([&server]() {
        server->start();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    constexpr size_t num_clients = 8;
    constexpr size_t reqs_per_client = 1000;
    std::atomic<size_t> success_count{0};

    std::vector<std::thread> client_threads;
    for (size_t c = 0; c < num_clients; ++c) {
        client_threads.emplace_back([c, &success_count]() {
            RedisClient client;
            if (!client.connect("127.0.0.1", 16380)) {
                return;
            }

            for (size_t i = 0; i < reqs_per_client; ++i) {
                std::string k = "c_key:" + std::to_string(c) + ":" + std::to_string(i);
                std::string v = "val:" + std::to_string(i);

                auto set_res = client.execute({"SET", k, v});
                if (set_res.type == RespType::SIMPLE_STRING && set_res.get_string() == "OK") {
                    auto get_res = client.execute({"GET", k});
                    if (get_res.type == RespType::BULK_STRING && get_res.get_string() == v) {
                        success_count.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }

    for (auto& t : client_threads) {
        t.join();
    }

    assert(success_count.load() == num_clients * reqs_per_client);
    std::cout << "[PASS] test_concurrent_tcp_load (" << success_count.load() << " verified concurrent operations)\n";

    server->stop();
    if (server_thread.joinable()) {
        server_thread.join();
    }
}

int main() {
    std::cout << "Running Concurrent TCP Load Integration Tests...\n";
    test_concurrent_tcp_load();
    std::cout << "All Concurrent Load Integration Tests Passed!\n";
    return 0;
}
