#pragma once

#include "net/poller.hpp"
#include <poll.h>
#include <unordered_map>

namespace miniredis {

class PollPoller : public Poller {
public:
    PollPoller();
    ~PollPoller() override = default;

    bool add_fd(int fd, uint32_t events, void* udata = nullptr) override;
    bool mod_fd(int fd, uint32_t events, void* udata = nullptr) override;
    bool del_fd(int fd) override;
    int poll(int timeout_ms, std::vector<FdEvent>& active_events) override;

private:
    struct FdContext {
        uint32_t events{0};
        void* udata{nullptr};
        size_t poll_index{0};
    };

    std::vector<struct pollfd> poll_fds_;
    std::unordered_map<int, FdContext> fd_map_;
};

} // namespace miniredis
