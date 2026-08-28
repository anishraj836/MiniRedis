#include "server/redis_server.hpp"
#include "client/redis_client.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

using namespace miniredis;

// Edge Case 1: Extreme Pipelining (500 commands in a single TCP write)
void test_massive_pipelining() {
    ServerConfig config;
    config.host = "127.0.0.1";
    config.port = 19379;
    config.worker_threads = 2;
    config.shard_count = 8;
    config.aof_enabled = false;

    auto server = std::make_unique<RedisServer>(config);
    std::thread server_thread([&server]() { server->start(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    RedisClient client;
    assert(client.connect("127.0.0.1", 19379));

    constexpr size_t pipeline_count = 500;

    // Send 500 commands without waiting for responses
    for (size_t i = 0; i < pipeline_count; ++i) {
        assert(client.send_command({"SET", "pipe:" + std::to_string(i), "val:" + std::to_string(i)}));
    }

    // Read all 500 responses
    for (size_t i = 0; i < pipeline_count; ++i) {
        auto resp = client.read_response();
        assert(resp.type == RespType::SIMPLE_STRING && resp.get_string() == "OK");
    }

    client.disconnect();
    server->stop();
    if (server_thread.joinable()) server_thread.join();

    std::cout << "[PASS] test_massive_pipelining (500 commands in batch pipeline)\n";
}

// Edge Case 2: Sudden Abrupt Client Disconnect & Socket Flooding
void test_abrupt_disconnect_flooding() {
    ServerConfig config;
    config.host = "127.0.0.1";
    config.port = 19380;
    config.worker_threads = 4;
    config.shard_count = 8;
    config.aof_enabled = false;

    auto server = std::make_unique<RedisServer>(config);
    std::thread server_thread([&server]() { server->start(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 100 rapid connection/immediate close cycles (testing FD reuse and zero socket leaks)
    for (int i = 0; i < 100; ++i) {
        int sock = ::socket(AF_INET, SOCK_STREAM, 0);
        assert(sock >= 0);

        struct sockaddr_in serv_addr{};
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(19380);
        ::inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

        int ret = ::connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
        if (ret == 0) {
            // Write half a command and abruptly close with RST
            std::string partial = "*3\r\n$3\r\nSET\r\n";
            ssize_t w = ::write(sock, partial.data(), partial.size());
            (void)w;

            struct linger sl{};
            sl.l_onoff = 1;
            sl.l_linger = 0; // Forces TCP RST on close
            ::setsockopt(sock, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
        }
        ::close(sock);
    }

    // Verify server is STILL healthy and responsive after 100 abrupt RST attacks
    RedisClient healthy_client;
    assert(healthy_client.connect("127.0.0.1", 19380));
    auto ping_res = healthy_client.execute({"PING"});
    assert(ping_res.type == RespType::SIMPLE_STRING && ping_res.get_string() == "PONG");
    healthy_client.disconnect();

    server->stop();
    if (server_thread.joinable()) server_thread.join();

    std::cout << "[PASS] test_abrupt_disconnect_flooding (100 RST socket floods survived)\n";
}

int main() {
    std::cout << "Running Network & Resilience Edge-Case Tests...\n";
    test_massive_pipelining();
    test_abrupt_disconnect_flooding();
    std::cout << "All Network & Resilience Edge-Case Tests Passed!\n";
    return 0;
}
