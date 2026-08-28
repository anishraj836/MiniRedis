#include "server/redis_server.hpp"
#include "client/redis_client.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>

using namespace miniredis;

void test_live_tcp_server() {
    ServerConfig config;
    config.host = "127.0.0.1";
    config.port = 16379;
    config.worker_threads = 2;
    config.shard_count = 8;
    config.aof_enabled = false;

    auto server = std::make_unique<RedisServer>(config);
    std::thread server_thread([&server]() {
        server->start();
    });

    // Wait for server to bind port
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    RedisClient client;
    bool connected = false;
    for (int retry = 0; retry < 10; ++retry) {
        if (client.connect("127.0.0.1", 16379)) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    assert(connected);

    // 1. Test PING
    auto ping_res = client.execute({"PING"});
    assert(ping_res.type == RespType::SIMPLE_STRING && ping_res.get_string() == "PONG");

    // 2. Test SET & GET
    auto set_res = client.execute({"SET", "server_key", "server_value"});
    assert(set_res.type == RespType::SIMPLE_STRING && set_res.get_string() == "OK");

    auto get_res = client.execute({"GET", "server_key"});
    assert(get_res.type == RespType::BULK_STRING && get_res.get_string() == "server_value");

    // 3. Test INCR
    auto incr_res = client.execute({"INCR", "counter"});
    assert(incr_res.type == RespType::INTEGER && incr_res.get_integer() == 1);

    auto incr_res2 = client.execute({"INCR", "counter"});
    assert(incr_res2.type == RespType::INTEGER && incr_res2.get_integer() == 2);

    // 4. Test Hashes
    auto hset_res = client.execute({"HSET", "user:99", "name", "Bob", "city", "Seattle"});
    assert(hset_res.type == RespType::INTEGER && hset_res.get_integer() == 2);

    auto hget_res = client.execute({"HGET", "user:99", "name"});
    assert(hget_res.type == RespType::BULK_STRING && hget_res.get_string() == "Bob");

    // 5. Test Lists
    auto lpush_res = client.execute({"LPUSH", "mylist", "a", "b", "c"});
    assert(lpush_res.type == RespType::INTEGER && lpush_res.get_integer() == 3);

    auto lrange_res = client.execute({"LRANGE", "mylist", "0", "-1"});
    assert(lrange_res.type == RespType::ARRAY && lrange_res.get_array().size() == 3);

    // 6. Test Expiration
    client.execute({"SET", "exp_key", "val"});
    auto exp_res = client.execute({"EXPIRE", "exp_key", "100"});
    assert(exp_res.type == RespType::INTEGER && exp_res.get_integer() == 1);

    auto ttl_res = client.execute({"TTL", "exp_key"});
    assert(ttl_res.type == RespType::INTEGER && ttl_res.get_integer() > 0);

    // 7. Test INFO
    auto info_res = client.execute({"INFO"});
    assert(info_res.type == RespType::BULK_STRING && info_res.get_string().find("miniredis_version") != std::string::npos);

    // Clean disconnect and server stop
    client.disconnect();
    server->stop();
    if (server_thread.joinable()) {
        server_thread.join();
    }

    std::cout << "[PASS] test_live_tcp_server (all live socket commands succeeded)\n";
}

int main() {
    std::cout << "Running Server Integration Tests...\n";
    test_live_tcp_server();
    std::cout << "All Server Integration Tests Passed!\n";
    return 0;
}
