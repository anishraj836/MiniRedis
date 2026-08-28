#include "client/redis_client.hpp"
#include "common/types.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <iomanip>
#include <cmath>

struct BenchConfig {
    std::string host{"127.0.0.1"};
    uint16_t port{6379};
    size_t clients{50};
    size_t total_requests{100000};
    size_t data_size{32};
    size_t threads{4};
    size_t pipeline{1};
    std::string test_type{"ALL"};
};

struct LatencyStats {
    std::vector<double> latencies_ms;
    size_t completed_requests{0};
    size_t error_count{0};
};

void run_benchmark_test(const std::string& test_name,
                        const BenchConfig& config,
                        const std::function<std::vector<std::string>(size_t req_idx)>& cmd_generator) {
    std::cout << "\n====== " << test_name << " ======\n";
    std::cout << "  " << config.total_requests << " requests completed in concurrent mode\n";
    std::cout << "  " << config.clients << " parallel clients, " << config.threads << " worker threads, pipeline=" << config.pipeline << "\n";

    std::vector<std::thread> workers;
    std::vector<LatencyStats> worker_stats(config.threads);
    std::atomic<size_t> req_counter{0};

    size_t clients_per_thread = std::max<size_t>(1, config.clients / config.threads);

    auto start_time = std::chrono::steady_clock::now();

    for (size_t t = 0; t < config.threads; ++t) {
        workers.emplace_back([t, &config, &req_counter, &worker_stats, &cmd_generator, clients_per_thread]() {
            auto& stats = worker_stats[t];
            stats.latencies_ms.reserve(config.total_requests / config.threads + 1000);

            std::vector<miniredis::RedisClient> clients(clients_per_thread);
            for (auto& cl : clients) {
                cl.connect(config.host, config.port);
            }

            size_t client_idx = 0;
            while (true) {
                size_t req_idx = req_counter.fetch_add(config.pipeline, std::memory_order_relaxed);
                if (req_idx >= config.total_requests) {
                    break;
                }

                size_t batch_size = std::min(config.pipeline, config.total_requests - req_idx);
                auto& client = clients[client_idx];
                client_idx = (client_idx + 1) % clients.size();

                auto req_start = std::chrono::steady_clock::now();

                if (batch_size == 1) {
                    auto cmd = cmd_generator(req_idx);
                    auto resp = client.execute(cmd);
                    auto req_end = std::chrono::steady_clock::now();
                    double lat = std::chrono::duration<double, std::milli>(req_end - req_start).count();
                    stats.latencies_ms.push_back(lat);
                    if (resp.type == miniredis::RespType::ERROR) {
                        ++stats.error_count;
                    }
                    ++stats.completed_requests;
                } else {
                    // Pipelined execution
                    for (size_t b = 0; b < batch_size; ++b) {
                        auto cmd = cmd_generator(req_idx + b);
                        client.send_command(cmd);
                    }
                    for (size_t b = 0; b < batch_size; ++b) {
                        auto resp = client.read_response();
                        if (resp.type == miniredis::RespType::ERROR) {
                            ++stats.error_count;
                        }
                        ++stats.completed_requests;
                    }
                    auto req_end = std::chrono::steady_clock::now();
                    double lat = std::chrono::duration<double, std::milli>(req_end - req_start).count() / batch_size;
                    for (size_t b = 0; b < batch_size; ++b) {
                        stats.latencies_ms.push_back(lat);
                    }
                }
            }
        });
    }

    for (auto& w : workers) {
        if (w.joinable()) w.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    double total_sec = std::chrono::duration<double>(end_time - start_time).count();

    // Aggregate statistics
    std::vector<double> all_latencies;
    size_t total_done = 0;
    size_t total_errs = 0;

    for (const auto& ws : worker_stats) {
        total_done += ws.completed_requests;
        total_errs += ws.error_count;
        all_latencies.insert(all_latencies.end(), ws.latencies_ms.begin(), ws.latencies_ms.end());
    }

    std::sort(all_latencies.begin(), all_latencies.end());

    double qps = total_sec > 0 ? (total_done / total_sec) : 0;
    double p50 = all_latencies.empty() ? 0 : all_latencies[all_latencies.size() * 50 / 100];
    double p90 = all_latencies.empty() ? 0 : all_latencies[all_latencies.size() * 90 / 100];
    double p95 = all_latencies.empty() ? 0 : all_latencies[all_latencies.size() * 95 / 100];
    double p99 = all_latencies.empty() ? 0 : all_latencies[all_latencies.size() * 99 / 100];
    double p999 = all_latencies.empty() ? 0 : all_latencies[all_latencies.size() * 999 / 1000];
    double min_lat = all_latencies.empty() ? 0 : all_latencies.front();
    double max_lat = all_latencies.empty() ? 0 : all_latencies.back();

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  Throughput: \033[32m" << qps << " requests per second\033[0m\n";
    std::cout << "  Total Time: " << total_sec << " seconds (Errors: " << total_errs << ")\n";
    std::cout << "  Latency percentiles:\n";
    std::cout << "    min:   " << min_lat << " ms\n";
    std::cout << "    p50:   " << p50 << " ms\n";
    std::cout << "    p90:   " << p90 << " ms\n";
    std::cout << "    p95:   " << p95 << " ms\n";
    std::cout << "    p99:   " << p99 << " ms\n";
    std::cout << "    p99.9: " << p999 << " ms\n";
    std::cout << "    max:   " << max_lat << " ms\n";
}

int main(int argc, char* argv[]) {
    BenchConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            config.host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if ((arg == "-c" || arg == "--clients") && i + 1 < argc) {
            config.clients = static_cast<size_t>(std::atoi(argv[++i]));
        } else if ((arg == "-n" || arg == "--requests") && i + 1 < argc) {
            config.total_requests = static_cast<size_t>(std::atoi(argv[++i]));
        } else if ((arg == "-d" || arg == "--datasize") && i + 1 < argc) {
            config.data_size = static_cast<size_t>(std::atoi(argv[++i]));
        } else if ((arg == "-t" || arg == "--threads") && i + 1 < argc) {
            config.threads = static_cast<size_t>(std::atoi(argv[++i]));
        } else if ((arg == "-P" || arg == "--pipeline") && i + 1 < argc) {
            config.pipeline = static_cast<size_t>(std::atoi(argv[++i]));
        } else if ((arg == "-k" || arg == "--test") && i + 1 < argc) {
            config.test_type = argv[++i];
        }
    }

    std::string payload(config.data_size, 'x');

    std::cout << "==================================================================\n";
    std::cout << "              MiniRedis High-Performance Benchmark Suite           \n";
    std::cout << "==================================================================\n";
    std::cout << "Target: " << config.host << ":" << config.port << "\n";
    std::cout << "Total Requests: " << config.total_requests << ", Clients: " << config.clients << ", Data Size: " << config.data_size << " bytes\n";

    // PING
    if (config.test_type == "ALL" || config.test_type == "PING") {
        run_benchmark_test("PING_INLINE", config, [](size_t) {
            return std::vector<std::string>{"PING"};
        });
    }

    // SET
    if (config.test_type == "ALL" || config.test_type == "SET") {
        run_benchmark_test("SET", config, [&payload](size_t i) {
            return std::vector<std::string>{"SET", "key:" + std::to_string(i), payload};
        });
    }

    // GET
    if (config.test_type == "ALL" || config.test_type == "GET") {
        run_benchmark_test("GET", config, [](size_t i) {
            return std::vector<std::string>{"GET", "key:" + std::to_string(i)};
        });
    }

    // INCR
    if (config.test_type == "ALL" || config.test_type == "INCR") {
        run_benchmark_test("INCR", config, [](size_t i) {
            return std::vector<std::string>{"INCR", "counter:" + std::to_string(i % 1000)};
        });
    }

    // LPUSH
    if (config.test_type == "ALL" || config.test_type == "LPUSH") {
        run_benchmark_test("LPUSH", config, [&payload](size_t i) {
            return std::vector<std::string>{"LPUSH", "mylist:" + std::to_string(i % 100), payload};
        });
    }

    // LPOP
    if (config.test_type == "ALL" || config.test_type == "LPOP") {
        run_benchmark_test("LPOP", config, [](size_t i) {
            return std::vector<std::string>{"LPOP", "mylist:" + std::to_string(i % 100)};
        });
    }

    // SADD
    if (config.test_type == "ALL" || config.test_type == "SADD") {
        run_benchmark_test("SADD", config, [](size_t i) {
            return std::vector<std::string>{"SADD", "myset:" + std::to_string(i % 100), "member:" + std::to_string(i)};
        });
    }

    // HSET
    if (config.test_type == "ALL" || config.test_type == "HSET") {
        run_benchmark_test("HSET", config, [&payload](size_t i) {
            return std::vector<std::string>{"HSET", "myhash:" + std::to_string(i % 100), "field:" + std::to_string(i), payload};
        });
    }

    // LRANGE
    if (config.test_type == "ALL" || config.test_type == "LRANGE") {
        run_benchmark_test("LRANGE_100", config, [](size_t i) {
            return std::vector<std::string>{"LRANGE", "mylist:" + std::to_string(i % 100), "0", "99"};
        });
    }

    // MSET
    if (config.test_type == "ALL" || config.test_type == "MSET") {
        run_benchmark_test("MSET (10 keys)", config, [&payload](size_t i) {
            std::vector<std::string> cmd = {"MSET"};
            for (int k = 0; k < 10; ++k) {
                cmd.push_back("mkey:" + std::to_string(i * 10 + k));
                cmd.push_back(payload);
            }
            return cmd;
        });
    }

    std::cout << "\n==================================================================\n";
    std::cout << "                      Benchmark Finished!                         \n";
    std::cout << "==================================================================\n";

    return 0;
}
