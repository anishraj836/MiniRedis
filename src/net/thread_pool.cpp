#include "net/thread_pool.hpp"
#include "common/logger.hpp"

namespace miniredis {

ThreadPool::ThreadPool(EventLoop* base_loop, size_t num_threads)
    : base_loop_(base_loop),
      num_threads_(num_threads) {}

ThreadPool::~ThreadPool() {
    stop();
}

void ThreadPool::start() {
    if (started_.exchange(true)) return;

    if (num_threads_ == 0) {
        loops_.push_back(base_loop_);
        return;
    }

    owned_loops_.reserve(num_threads_);
    threads_.reserve(num_threads_);

    for (size_t i = 0; i < num_threads_; ++i) {
        auto loop = std::make_unique<EventLoop>();
        EventLoop* loop_ptr = loop.get();
        owned_loops_.push_back(std::move(loop));
        loops_.push_back(loop_ptr);

        threads_.emplace_back([loop_ptr]() {
            loop_ptr->loop();
        });
    }
}

void ThreadPool::stop() {
    if (!started_.exchange(false)) return;

    for (auto& loop : owned_loops_) {
        loop->stop();
    }

    for (auto& t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    threads_.clear();
    owned_loops_.clear();
    loops_.clear();
}

EventLoop* ThreadPool::get_next_loop() {
    if (loops_.empty()) {
        return base_loop_;
    }
    EventLoop* loop = loops_[next_loop_idx_];
    next_loop_idx_ = (next_loop_idx_ + 1) % loops_.size();
    return loop;
}

} // namespace miniredis
