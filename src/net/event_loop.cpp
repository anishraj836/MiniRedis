#include "net/event_loop.hpp"
#include "common/logger.hpp"
#include "common/types.hpp"
#include "net/connection.hpp"
#include <unistd.h>
#include <fcntl.h>
#include <cassert>
#include <cerrno>
#include <cstring>

namespace miniredis {

EventLoop::EventLoop()
    : poller_(Poller::create()) {
    // Create non-blocking pipe for inter-thread wakeups
    if (::pipe(wakeup_pipe_) < 0) {
        LOG_FATAL("Failed to create wakeup pipe: " + std::string(strerror(errno)));
    }

    // Set non-blocking on both pipe ends
    fcntl(wakeup_pipe_[0], F_SETFL, fcntl(wakeup_pipe_[0], F_GETFL, 0) | O_NONBLOCK);
    fcntl(wakeup_pipe_[1], F_SETFL, fcntl(wakeup_pipe_[1], F_GETFL, 0) | O_NONBLOCK);

    // Register read end in poller
    poller_->add_fd(wakeup_pipe_[0], EventMask::READ, nullptr);
}

EventLoop::~EventLoop() {
    stop();
    if (wakeup_pipe_[0] >= 0) {
        poller_->del_fd(wakeup_pipe_[0]);
        ::close(wakeup_pipe_[0]);
    }
    if (wakeup_pipe_[1] >= 0) {
        ::close(wakeup_pipe_[1]);
    }
}

void EventLoop::assert_in_loop_thread() const {
    if (!is_in_loop_thread()) {
        LOG_FATAL("EventLoop thread affinity assertion failed!");
    }
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeup_pipe_[1], &one, sizeof(one));
    (void)n;
}

void EventLoop::handle_wakeup() {
    uint64_t buf[16];
    while (::read(wakeup_pipe_[0], buf, sizeof(buf)) > 0) {
        // Drain pipe
    }
}

void EventLoop::run_in_loop(Task task) {
    if (is_in_loop_thread()) {
        task();
    } else {
        queue_in_loop(std::move(task));
    }
}

void EventLoop::queue_in_loop(Task task) {
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        pending_tasks_.push_back(std::move(task));
    }
    if (!is_in_loop_thread() || calling_pending_tasks_) {
        wakeup();
    }
}

void EventLoop::run_every(int interval_ms, TimerCallback cb) {
    int64_t next = now_ms() + interval_ms;
    timers_.push_back({next, interval_ms, std::move(cb)});
}

bool EventLoop::update_channel(int fd, uint32_t events, void* udata) {
    return poller_->mod_fd(fd, events, udata);
}

bool EventLoop::remove_channel(int fd) {
    return poller_->del_fd(fd);
}

void EventLoop::check_timers() {
    if (timers_.empty()) return;
    int64_t current = now_ms();
    for (auto& timer : timers_) {
        if (current >= timer.next_run_ms) {
            timer.cb();
            timer.next_run_ms = current + timer.interval_ms;
        }
    }
}

void EventLoop::do_pending_tasks() {
    std::vector<Task> tasks;
    calling_pending_tasks_ = true;
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        tasks.swap(pending_tasks_);
    }

    for (const auto& task : tasks) {
        task();
    }
    calling_pending_tasks_ = false;
}

void EventLoop::loop() {
    looping_ = true;
    quit_ = false;
    thread_id_ = std::this_thread::get_id();

    std::vector<FdEvent> active_events;
    active_events.reserve(256);

    LOG_DEBUG("EventLoop started");

    while (!quit_) {
        active_events.clear();
        int ret = poller_->poll(10, active_events); // 10ms timeout for timer responsiveness
        if (ret > 0) {
            for (const auto& ev : active_events) {
                if (ev.fd == wakeup_pipe_[0]) {
                    handle_wakeup();
                } else if (ev.udata != nullptr) {
                    auto* conn = static_cast<Connection*>(ev.udata);
                    conn->handle_events(ev.events);
                }
            }
        }

        check_timers();
        do_pending_tasks();
    }

    looping_ = false;
    LOG_DEBUG("EventLoop exited cleanly");
}

void EventLoop::stop() {
    quit_ = true;
    wakeup();
}

} // namespace miniredis
