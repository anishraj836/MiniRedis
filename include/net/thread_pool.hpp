#pragma once

#include "net/event_loop.hpp"
#include <vector>
#include <memory>
#include <thread>
#include <atomic>

namespace miniredis {

class ThreadPool {
public:
    explicit ThreadPool(EventLoop* base_loop, size_t num_threads = 0);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void set_thread_num(size_t num) noexcept { num_threads_ = num; }
    void start();
    void stop();

    [[nodiscard]] EventLoop* get_next_loop();
    [[nodiscard]] const std::vector<EventLoop*>& get_all_loops() const noexcept { return loops_; }
    [[nodiscard]] size_t size() const noexcept { return loops_.size(); }

private:
    EventLoop* base_loop_;
    size_t num_threads_{0};
    std::atomic<bool> started_{false};
    size_t next_loop_idx_{0};

    std::vector<std::unique_ptr<EventLoop>> owned_loops_;
    std::vector<EventLoop*> loops_;
    std::vector<std::thread> threads_;
};

} // namespace miniredis
