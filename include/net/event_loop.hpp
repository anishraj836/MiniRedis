#pragma once

#include "net/poller.hpp"
#include <functional>
#include <vector>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>

namespace miniredis {

class Connection;

class EventLoop {
public:
    using Task = std::function<void()>;
    using TimerCallback = std::function<void()>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void loop();
    void stop();

    // Thread dispatching
    void run_in_loop(Task task);
    void queue_in_loop(Task task);

    // Periodic timers
    void run_every(int interval_ms, TimerCallback cb);

    // Channel/Poller interface
    bool update_channel(int fd, uint32_t events, void* udata);
    bool remove_channel(int fd);

    [[nodiscard]] bool is_in_loop_thread() const noexcept {
        return thread_id_.load(std::memory_order_acquire) == std::this_thread::get_id();
    }

    void assert_in_loop_thread() const;

    void wakeup();

private:
    void handle_wakeup();
    void do_pending_tasks();
    void check_timers();

    struct TimerItem {
        int64_t next_run_ms;
        int interval_ms;
        TimerCallback cb;
    };

    std::atomic<bool> looping_{false};
    std::atomic<bool> quit_{false};
    // Written by the loop thread when loop() starts, read by any thread that
    // posts work to this loop: must be atomic.
    std::atomic<std::thread::id> thread_id_{};

    std::unique_ptr<Poller> poller_;
    int wakeup_pipe_[2]{-1, -1};

    std::mutex task_mutex_;
    std::vector<Task> pending_tasks_;
    std::atomic<bool> calling_pending_tasks_{false};

    std::vector<TimerItem> timers_;
};

} // namespace miniredis
