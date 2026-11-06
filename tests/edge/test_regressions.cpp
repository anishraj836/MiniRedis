// Regression tests for bugs found during the bug hunt. Each test reproduces
// the original failure; it fails on the old code and passes on the fix.

#include "server/redis_server.hpp"
#include "server/command_registry.hpp"
#include "server/command_rewrite.hpp"
#include "client/redis_client.hpp"
#include "aof/aof_logger.hpp"
#include "aof/aof_recovery.hpp"
#include "aof/aof_rewriter.hpp"
#include "protocol/resp_parser.hpp"
#include "storage/storage_engine.hpp"
#include "storage/ttl_manager.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <chrono>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace miniredis;
using namespace std::chrono_literals;

namespace {

// ---------------------------------------------------------------- helpers

RespValue run(CommandRegistry& reg, StorageEngine& engine, std::vector<std::string> tokens) {
    return reg.dispatch(Command(std::move(tokens)), engine, nullptr);
}

bool is_error(const RespValue& v, const std::string& prefix) {
    return v.type == RespType::ERROR && v.get_string().rfind(prefix, 0) == 0;
}

int count_open_fds() {
    int n = 0;
    for (int fd = 0; fd < 4096; ++fd) {
        if (::fcntl(fd, F_GETFD) != -1) ++n;
    }
    return n;
}

int raw_connect(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    timeval tv{2, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    return fd;
}

// Sends `out`, then reads until the peer closes or the read times out.
std::string raw_send_and_drain(uint16_t port, const std::string& out) {
    int fd = raw_connect(port);
    assert(fd >= 0);
    ssize_t w = ::send(fd, out.data(), out.size(), 0);
    (void)w;
    std::string in;
    char buf[4096];
    while (true) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        in.append(buf, static_cast<size_t>(n));
    }
    ::close(fd);
    return in;
}

class TestServer {
public:
    explicit TestServer(ServerConfig config) : port_(config.port) {
        server_ = std::make_unique<RedisServer>(std::move(config));
        thread_ = std::thread([this] { server_->start(); });
        for (int i = 0; i < 100; ++i) {
            int fd = raw_connect(port_);
            if (fd >= 0) {
                ::close(fd);
                return;
            }
            std::this_thread::sleep_for(20ms);
        }
        assert(false && "server did not start");
    }
    ~TestServer() { stop(); }

    void stop() {
        if (!server_) return;
        server_->stop();
        thread_.join();
        server_.reset();
    }

    void connect(RedisClient& c) const {
        bool ok = c.connect("127.0.0.1", port_);
        assert(ok);
        (void)ok;
    }

private:
    uint16_t port_;
    std::unique_ptr<RedisServer> server_;
    std::thread thread_;
};

ServerConfig config_for(uint16_t port, bool aof = false, const std::string& aof_file = "") {
    ServerConfig c;
    c.host = "127.0.0.1";
    c.port = port;
    c.worker_threads = 4;
    c.shard_count = 16;
    c.aof_enabled = aof;
    if (aof) {
        c.aof_filename = aof_file;
        c.aof_fsync_policy = AofFsyncPolicy::NO;
    }
    return c;
}

// --------------------------------------------------------- parser bugs

void test_parser_rejects_huge_array_header() {
    // "*9223372036854775807\r\n" made arr.reserve() throw std::length_error,
    // terminating the whole server with a 22-byte packet.
    RespParser parser;
    ByteBuffer buf;
    buf.append("*9223372036854775807\r\n");
    std::vector<std::string> cmd;
    assert(parser.parse_command(buf, cmd) == ParseResult::ERROR);

    ByteBuffer buf2;
    buf2.append("*9223372036854775807\r\n");
    RespValue v;
    assert(parser.parse(buf2, v) == ParseResult::ERROR);
    std::cout << "[PASS] test_parser_rejects_huge_array_header\n";
}

void test_parser_rejects_deep_nesting() {
    // Unbounded recursion on "*1\r\n*1\r\n..." overflowed the stack.
    std::string nested;
    for (int i = 0; i < 100000; ++i) nested += "*1\r\n";
    RespParser parser;
    ByteBuffer buf;
    buf.append(nested);
    RespValue v;
    assert(parser.parse(buf, v) == ParseResult::ERROR);

    ByteBuffer buf2;
    buf2.append(nested);
    std::vector<std::string> cmd;
    assert(parser.parse_command(buf2, cmd) == ParseResult::ERROR);
    std::cout << "[PASS] test_parser_rejects_deep_nesting\n";
}

void test_parser_limits_buffering() {
    RespParser parser;
    std::vector<std::string> cmd;

    // A bulk length beyond 512 MB used to make the server buffer forever.
    ByteBuffer big;
    big.append("*1\r\n$999999999999\r\n");
    assert(parser.parse_command(big, cmd) == ParseResult::ERROR);

    // An inline line with no newline used to be buffered without limit.
    ByteBuffer line;
    line.append(std::string(RespParser::MAX_INLINE_LENGTH + 10, 'a'));
    assert(parser.parse_command(line, cmd) == ParseResult::ERROR);

    // Bulk payload longer than its declared length.
    ByteBuffer bad_trailer;
    bad_trailer.append("*1\r\n$3\r\nGETX\r\n");
    assert(parser.parse_command(bad_trailer, cmd) == ParseResult::ERROR);
    std::cout << "[PASS] test_parser_limits_buffering\n";
}

void test_parser_incomplete_is_not_consumed() {
    RespParser parser;
    ByteBuffer buf;
    std::string frame = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
    for (size_t cut = 1; cut < frame.size(); ++cut) {
        ByteBuffer partial;
        partial.append(frame.substr(0, cut));
        std::vector<std::string> cmd;
        assert(parser.parse_command(partial, cmd) == ParseResult::INCOMPLETE);
        assert(partial.readable_bytes() == cut); // nothing consumed
    }
    std::cout << "[PASS] test_parser_incomplete_is_not_consumed\n";
}

void test_parser_empty_frames_do_not_stall_pipeline() {
    // "*0\r\n" and blank lines returned INCOMPLETE, so the command behind them
    // waited for the next network read.
    RespParser parser;
    ByteBuffer buf;
    buf.append("*0\r\n\r\n*1\r\n$4\r\nPING\r\n");
    std::vector<std::string> cmd;
    assert(parser.parse_command(buf, cmd) == ParseResult::OK);
    assert(cmd.size() == 1 && cmd[0] == "PING");
    std::cout << "[PASS] test_parser_empty_frames_do_not_stall_pipeline\n";
}

// --------------------------------------------------------- storage bugs

void test_memory_accounting_balances_for_collections() {
    // Emptying a hash/list/set via HDEL/LPOP/SREM never subtracted the last
    // element's bytes, and collections were never charged for their key, so
    // used_memory drifted with every operation.
    StorageEngine engine(4, 0, EvictionPolicy::NO_EVICTION);

    for (int round = 0; round < 50; ++round) {
        std::string r = std::to_string(round);
        engine.get_shard("h" + r).hset("h" + r, {{"field", "value-" + r}, {"f2", std::string(100, 'x')}});
        engine.get_shard("l" + r).rpush("l" + r, {"a", std::string(64, 'b'), "c"});
        engine.get_shard("s" + r).sadd("s" + r, {"m1", "m2", std::string(40, 'm')});
        engine.set_string("str" + r, std::string(round * 10, 'z'));
        assert(engine.verify_memory_accounting());

        engine.get_shard("h" + r).hdel("h" + r, "field");
        engine.get_shard("h" + r).hdel("h" + r, "f2");
        engine.get_shard("l" + r).lpop("l" + r);
        engine.get_shard("l" + r).rpop("l" + r);
        engine.get_shard("l" + r).lpop("l" + r);
        engine.get_shard("s" + r).srem("s" + r, {"m1", "m2", std::string(40, 'm')});
        engine.del("str" + r);
        assert(engine.verify_memory_accounting());
    }

    assert(engine.total_keys() == 0);
    assert(engine.total_memory_bytes() == 0);
    std::cout << "[PASS] test_memory_accounting_balances_for_collections\n";
}

void test_collections_respect_maxmemory() {
    // maxmemory was only enforced by SET; RPUSH/HSET/SADD grew without limit.
    StorageEngine engine(1, 4096, EvictionPolicy::NO_EVICTION);
    auto& shard = engine.get_shard("list");
    int64_t res = 0;
    for (int i = 0; i < 1000 && res >= 0; ++i) {
        res = shard.rpush("list", {std::string(100, 'x')});
    }
    assert(res == StorageShard::OUT_OF_MEMORY);
    assert(engine.total_memory_bytes() <= 4096);
    assert(engine.verify_memory_accounting());
    std::cout << "[PASS] test_collections_respect_maxmemory\n";
}

void test_random_and_ttl_eviction_keep_indexes_consistent() {
    // allkeys-random / volatile-* eviction removed keys from the data map but
    // not from the LRU list. Stale LRU entries later made evict_one() fail and
    // writes report OOM while evictable keys still existed.
    for (EvictionPolicy policy : {EvictionPolicy::ALLKEYS_RANDOM, EvictionPolicy::VOLATILE_TTL,
                                  EvictionPolicy::VOLATILE_LRU}) {
        StorageEngine engine(1, 20000, policy);
        for (int i = 0; i < 2000; ++i) {
            bool ok = engine.set_string("k" + std::to_string(i), std::string(50, 'v'), now_ms() + 100000 + i);
            assert(ok);
            (void)ok;
        }
        assert(engine.verify_memory_accounting());
        assert(engine.memory_tracker().evicted_keys() > 0);
    }

    // volatile-ttl must evict the key closest to expiring.
    StorageEngine engine(1, 0, EvictionPolicy::VOLATILE_TTL);
    engine.set_string("late", "v", now_ms() + 900000);
    engine.set_string("soon", "v", now_ms() + 1000);
    engine.set_string("forever", "v");
    assert(engine.get_shard("x").evict_one(EvictionPolicy::VOLATILE_TTL));
    assert(!engine.exists("soon") && engine.exists("late") && engine.exists("forever"));
    std::cout << "[PASS] test_random_and_ttl_eviction_keep_indexes_consistent\n";
}

void test_active_expiry_reaches_every_expired_key() {
    // The sampler always looked at the *first* 20 entries of the TTL map. With
    // long-lived keys there, expired keys elsewhere were never reclaimed.
    StorageEngine engine(1);
    for (int i = 0; i < 200; ++i) engine.set_string("long:" + std::to_string(i), "v", now_ms() + 3600000);
    for (int i = 0; i < 2000; ++i) engine.set_string("short:" + std::to_string(i), "v", now_ms() + 20);
    std::this_thread::sleep_for(50ms);

    size_t swept = 0;
    for (int cycle = 0; cycle < 10; ++cycle) swept += engine.active_expire_cycle();
    assert(swept == 2000);
    assert(engine.total_keys() == 200);
    std::cout << "[PASS] test_active_expiry_reaches_every_expired_key\n";
}

void test_ttl_heap_does_not_grow_unbounded() {
    // Each EXPIRE pushed a heap entry and stale ones were only dropped once
    // their (future) deadline passed: refreshing a TTL in a loop leaked memory.
    TTLManager ttl;
    for (int i = 0; i < 200000; ++i) {
        ttl.set_expiry("session", now_ms() + 3600000 + i);
    }
    assert(ttl.size() == 1);
    assert(ttl.heap_size() < 5000);
    std::cout << "[PASS] test_ttl_heap_does_not_grow_unbounded\n";
}

void test_mset_is_all_or_nothing() {
    StorageEngine engine(8, 2000, EvictionPolicy::NO_EVICTION);
    std::vector<std::pair<std::string, std::string>> kvs;
    for (int i = 0; i < 10; ++i) kvs.emplace_back("m" + std::to_string(i), std::string(500, 'x'));
    assert(!engine.mset(kvs));
    assert(engine.total_keys() == 0); // nothing half-written
    std::cout << "[PASS] test_mset_is_all_or_nothing\n";
}

// --------------------------------------------------------- command bugs

void test_incr_on_wrong_type_does_not_destroy_data() {
    // INCR/APPEND on a hash read it as "missing" and overwrote it with a string.
    CommandRegistry reg;
    StorageEngine engine(4);
    run(reg, engine, {"HSET", "h", "f", "v"});
    assert(is_error(run(reg, engine, {"INCR", "h"}), "WRONGTYPE"));
    assert(is_error(run(reg, engine, {"APPEND", "h", "x"}), "WRONGTYPE"));
    assert(is_error(run(reg, engine, {"GET", "h"}), "WRONGTYPE"));
    assert(is_error(run(reg, engine, {"LPUSH", "h", "x"}), "WRONGTYPE"));
    assert(is_error(run(reg, engine, {"SMEMBERS", "h"}), "WRONGTYPE"));
    assert(engine.type("h") == RedisType::HASH);
    assert(engine.get_shard("h").hget("h", "f").value_or("") == "v");
    assert(run(reg, engine, {"TYPE", "h"}).get_string() == "hash"); // TYPE was documented but missing
    assert(run(reg, engine, {"TYPE", "nope"}).get_string() == "none");
    std::cout << "[PASS] test_incr_on_wrong_type_does_not_destroy_data\n";
}

void test_incr_and_append_keep_ttl() {
    CommandRegistry reg;
    StorageEngine engine(4);
    run(reg, engine, {"SET", "c", "1", "EX", "100"});
    run(reg, engine, {"INCR", "c"});
    run(reg, engine, {"APPEND", "c", "0"});
    assert(engine.get_string("c").value_or("") == "20");
    assert(engine.ttl_ms("c") > 90000); // used to become -1 (persistent)
    std::cout << "[PASS] test_incr_and_append_keep_ttl\n";
}

void test_integer_overflow_and_strict_parsing() {
    CommandRegistry reg;
    StorageEngine engine(4);
    run(reg, engine, {"SET", "n", "9223372036854775807"});
    assert(is_error(run(reg, engine, {"INCR", "n"}), "ERR"));            // was signed-overflow UB
    assert(engine.get_string("n").value_or("") == "9223372036854775807");
    assert(is_error(run(reg, engine, {"EXPIRE", "n", "9223372036854775807"}), "ERR"));
    assert(is_error(run(reg, engine, {"EXPIRE", "n", "10abc"}), "ERR"));

    run(reg, engine, {"HSET", "h", "f", "12abc"});
    assert(is_error(run(reg, engine, {"HINCRBY", "h", "f", "1"}), "ERR")); // "12abc" was read as 12
    std::cout << "[PASS] test_integer_overflow_and_strict_parsing\n";
}

void test_expireat_in_the_past_expires_key() {
    // A negative deadline was stored as "no TTL", making the key persistent.
    CommandRegistry reg;
    StorageEngine engine(4);
    run(reg, engine, {"SET", "k", "v"});
    run(reg, engine, {"EXPIREAT", "k", "-5"});
    assert(!engine.exists("k"));
    std::cout << "[PASS] test_expireat_in_the_past_expires_key\n";
}

void test_set_option_validation() {
    CommandRegistry reg;
    StorageEngine engine(4);
    assert(is_error(run(reg, engine, {"SET", "k", "v", "EX", "10", "PX", "100"}), "ERR syntax error"));
    assert(is_error(run(reg, engine, {"SET", "k", "v", "NX", "XX"}), "ERR syntax error"));
    assert(is_error(run(reg, engine, {"SET", "k", "v", "EX", "0"}), "ERR invalid expire"));
    // Value that looks like an option must be stored as a value.
    run(reg, engine, {"SET", "k", "EX"});
    assert(engine.get_string("k").value_or("") == "EX" && engine.ttl_ms("k") == -1);
    // GET / KEEPTTL
    run(reg, engine, {"SET", "k", "v1", "EX", "100"});
    RespValue old = run(reg, engine, {"SET", "k", "v2", "KEEPTTL", "GET"});
    assert(old.type == RespType::BULK_STRING && old.get_string() == "v1");
    assert(engine.ttl_ms("k") > 90000);
    std::cout << "[PASS] test_set_option_validation\n";
}

void test_config_set_takes_effect() {
    CommandRegistry reg;
    StorageEngine engine(4);
    assert(run(reg, engine, {"CONFIG", "SET", "maxmemory", "12345"}).type == RespType::SIMPLE_STRING);
    assert(engine.memory_tracker().get_max_memory() == 12345); // was a silent no-op
    assert(run(reg, engine, {"CONFIG", "SET", "maxmemory-policy", "allkeys-random"}).type == RespType::SIMPLE_STRING);
    assert(engine.memory_tracker().get_eviction_policy() == EvictionPolicy::ALLKEYS_RANDOM);
    assert(is_error(run(reg, engine, {"CONFIG", "SET", "maxmemory-policy", "bogus"}), "ERR"));
    std::cout << "[PASS] test_config_set_takes_effect\n";
}

void test_relative_expiry_is_logged_as_absolute() {
    const int64_t now = 1700000000000;
    auto e = normalize_for_propagation({"EXPIRE", "k", "10"}, now);
    assert(e && (*e == std::vector<std::string>{"PEXPIREAT", "k", "1700000010000"}));
    auto s = normalize_for_propagation({"SETEX", "k", "5", "v"}, now);
    assert(s && (*s == std::vector<std::string>{"SET", "k", "v", "PXAT", "1700000005000"}));
    auto p = normalize_for_propagation({"set", "k", "v", "NX", "px", "250"}, now);
    assert(p && (*p == std::vector<std::string>{"set", "k", "v", "NX", "PXAT", "1700000000250"}));
    assert(!normalize_for_propagation({"SET", "k", "v"}, now));
    assert(!normalize_for_propagation({"EXPIRE", "k", "nope"}, now)); // left for the handler to reject
    std::cout << "[PASS] test_relative_expiry_is_logged_as_absolute\n";
}

// --------------------------------------------------------- AOF bugs

void test_truncated_tail_is_repaired() {
    // A torn final write stopped replay there, but new appends went *after*
    // the garbage, so everything written after the restart was unreadable on
    // the following restart.
    const std::string file = "test_regress_trunc.aof";
    ::unlink(file.c_str());
    {
        int fd = ::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        std::string data = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$2\r\n";
        ssize_t w = ::write(fd, data.data(), data.size());
        (void)w;
        ::close(fd);
    }
    CommandRegistry reg;
    {
        StorageEngine engine(4);
        size_t n = 0;
        assert(AofRecovery::replay_file(file, engine, reg, n) && n == 1);
    }
    {
        AofLogger logger(file, AofFsyncPolicy::ALWAYS);
        assert(logger.open());
        logger.append({"SET", "c", "3"});
        logger.close();
    }
    {
        StorageEngine engine(4);
        size_t n = 0;
        assert(AofRecovery::replay_file(file, engine, reg, n) && n == 2);
        assert(engine.get_string("a").value_or("") == "1");
        assert(engine.get_string("c").value_or("") == "3");
    }
    ::unlink(file.c_str());
    std::cout << "[PASS] test_truncated_tail_is_repaired\n";
}

void test_corrupted_middle_refuses_to_load() {
    const std::string file = "test_regress_corrupt.aof";
    {
        int fd = ::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        std::string data = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n!garbage!\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
        ssize_t w = ::write(fd, data.data(), data.size());
        (void)w;
        ::close(fd);
    }
    CommandRegistry reg;
    StorageEngine engine(4);
    size_t n = 0;
    assert(!AofRecovery::replay_file(file, engine, reg, n)); // was: load half and continue
    ::unlink(file.c_str());
    std::cout << "[PASS] test_corrupted_middle_refuses_to_load\n";
}

// --------------------------------------------------------- server bugs

void test_no_fd_leak_per_connection() {
    // RedisServer replaced the TcpServer close hook on every connection, so
    // sockets were never closed: one leaked fd per client until accept() hit
    // the process fd limit (256 by default on macOS).
    TestServer server(config_for(16481));
    std::this_thread::sleep_for(100ms);
    int before = count_open_fds();
    for (int i = 0; i < 400; ++i) {
        std::string reply = raw_send_and_drain(16481, "PING\r\nQUIT\r\n");
        assert(reply.find("+PONG") != std::string::npos);
    }
    std::this_thread::sleep_for(300ms);
    int after = count_open_fds();
    assert(after - before < 10);
    std::cout << "[PASS] test_no_fd_leak_per_connection (" << before << " fds before, " << after << " after 400 clients)\n";
}

void test_concurrent_incr_and_setnx_are_atomic() {
    // INCR was GET + SET in two separate locks: concurrent clients lost
    // updates (2339 instead of 2400 in a manual run). SETNX had the same race,
    // so two clients could both "acquire" the same lock key.
    TestServer server(config_for(16482));
    constexpr int threads = 8;
    constexpr int per_thread = 400;
    std::atomic<int> setnx_wins{0};

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&] {
            RedisClient c; server.connect(c);
            for (int i = 0; i < per_thread; ++i) {
                c.execute({"INCR", "counter"});
                c.execute({"HINCRBY", "hash", "field", "1"});
                if (i < 50) {
                    RespValue r = c.execute({"SETNX", "lock:" + std::to_string(i), "owner"});
                    if (r.type == RespType::INTEGER && r.get_integer() == 1) ++setnx_wins;
                }
            }
        });
    }
    for (auto& w : workers) w.join();

    RedisClient c; server.connect(c);
    assert(c.execute({"GET", "counter"}).get_string() == std::to_string(threads * per_thread));
    assert(c.execute({"HGET", "hash", "field"}).get_string() == std::to_string(threads * per_thread));
    assert(setnx_wins == 50); // exactly one winner per lock key
    std::cout << "[PASS] test_concurrent_incr_and_setnx_are_atomic (" << threads * per_thread << " increments)\n";
}

void test_quit_flushes_pipelined_replies_and_stops_executing() {
    TestServer server(config_for(16483));
    // The reply to SET was dropped when QUIT arrived in the same packet.
    std::string reply = raw_send_and_drain(16483, "SET q 1\r\nQUIT\r\n");
    assert(reply == "+OK\r\n+OK\r\n");

    // Commands after QUIT must not run.
    raw_send_and_drain(16483, "QUIT\r\nSET after_quit 1\r\n");
    RedisClient c; server.connect(c);
    assert(c.execute({"EXISTS", "after_quit"}).get_integer() == 0);
    std::cout << "[PASS] test_quit_flushes_pipelined_replies_and_stops_executing\n";
}

void test_server_survives_malicious_frames() {
    TestServer server(config_for(16484));
    std::string reply = raw_send_and_drain(16484, "*9223372036854775807\r\n");
    assert(reply.find("-ERR Protocol error") == 0);

    std::string nested;
    for (int i = 0; i < 50000; ++i) nested += "*1\r\n";
    raw_send_and_drain(16484, nested);

    RedisClient c; server.connect(c);
    assert(c.execute({"PING"}).get_string() == "PONG");
    std::cout << "[PASS] test_server_survives_malicious_frames\n";
}

void test_expired_keys_stay_expired_after_restart() {
    // "SET k v PX 100" / "PEXPIRE k 100" were logged verbatim, so replay after
    // a restart gave the keys a fresh TTL and they came back to life.
    const std::string file = "test_regress_expiry.aof";
    ::unlink(file.c_str());
    {
        TestServer server(config_for(16485, true, file));
        RedisClient c; server.connect(c);
        c.execute({"SET", "a", "v", "PX", "100"});
        c.execute({"SET", "b", "v"});
        c.execute({"PEXPIRE", "b", "100"});
        c.execute({"SETEX", "c", "1", "v"});
        c.execute({"SET", "keep", "v"});
        std::this_thread::sleep_for(1200ms);
    }
    CommandRegistry reg;
    StorageEngine engine(16);
    size_t n = 0;
    assert(AofRecovery::replay_file(file, engine, reg, n));
    assert(!engine.exists("a") && !engine.exists("b") && !engine.exists("c"));
    assert(engine.exists("keep"));
    ::unlink(file.c_str());
    std::cout << "[PASS] test_expired_keys_stay_expired_after_restart\n";
}

void test_bgrewriteaof_under_load_is_exact() {
    // The online rewrite could both lose writes (flushed to the replaced file
    // between finishing the delta and reopening) and duplicate them (snapshot
    // taken while writers ran, plus buffered bytes copied into the new file).
    // For INCR / RPUSH that means a different dataset after restart.
    const std::string file = "test_regress_rewrite.aof";
    ::unlink(file.c_str());
    ::unlink((file + ".temp").c_str());
    std::string live_counter;
    int64_t live_len = 0;
    {
        TestServer server(config_for(16486, true, file));
        std::atomic<bool> done{false};
        std::vector<std::thread> writers;
        for (int t = 0; t < 4; ++t) {
            writers.emplace_back([&, t] {
                RedisClient c; server.connect(c);
                for (int i = 0; i < 1500; ++i) {
                    c.execute({"INCR", "counter"});
                    c.execute({"RPUSH", "list", std::to_string(t) + ":" + std::to_string(i)});
                    if (i % 100 == 0) c.execute({"DEL", "tmp:" + std::to_string(t)});
                    c.execute({"SET", "tmp:" + std::to_string(t), std::to_string(i)});
                }
            });
        }
        std::thread rewriter([&] {
            RedisClient c; server.connect(c);
            while (!done) {
                c.execute({"BGREWRITEAOF"});
                std::this_thread::sleep_for(15ms);
            }
        });
        for (auto& w : writers) w.join();
        done = true;
        rewriter.join();
        AofRewriter::wait_for_background_rewrite();

        RedisClient c; server.connect(c);
        live_counter = c.execute({"GET", "counter"}).get_string();
        live_len = c.execute({"LLEN", "list"}).get_integer();
    }
    assert(live_counter == "6000" && live_len == 6000);

    CommandRegistry reg;
    StorageEngine engine(16);
    size_t n = 0;
    assert(AofRecovery::replay_file(file, engine, reg, n));
    assert(engine.get_string("counter").value_or("") == live_counter);
    assert(static_cast<int64_t>(engine.get_shard("list").llen("list")) == live_len);
    ::unlink(file.c_str());
    std::cout << "[PASS] test_bgrewriteaof_under_load_is_exact (replayed " << n << " commands)\n";
}

} // anonymous namespace

int main() {
    std::cout << "Running Bug-Hunt Regression Tests...\n";
    test_parser_rejects_huge_array_header();
    test_parser_rejects_deep_nesting();
    test_parser_limits_buffering();
    test_parser_incomplete_is_not_consumed();
    test_parser_empty_frames_do_not_stall_pipeline();

    test_memory_accounting_balances_for_collections();
    test_collections_respect_maxmemory();
    test_random_and_ttl_eviction_keep_indexes_consistent();
    test_active_expiry_reaches_every_expired_key();
    test_ttl_heap_does_not_grow_unbounded();
    test_mset_is_all_or_nothing();

    test_incr_on_wrong_type_does_not_destroy_data();
    test_incr_and_append_keep_ttl();
    test_integer_overflow_and_strict_parsing();
    test_expireat_in_the_past_expires_key();
    test_set_option_validation();
    test_config_set_takes_effect();
    test_relative_expiry_is_logged_as_absolute();

    test_truncated_tail_is_repaired();
    test_corrupted_middle_refuses_to_load();

    test_no_fd_leak_per_connection();
    test_concurrent_incr_and_setnx_are_atomic();
    test_quit_flushes_pipelined_replies_and_stops_executing();
    test_server_survives_malicious_frames();
    test_expired_keys_stay_expired_after_restart();
    test_bgrewriteaof_under_load_is_exact();

    std::cout << "All Bug-Hunt Regression Tests Passed!\n";
    return 0;
}
