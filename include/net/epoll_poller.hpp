#pragma once

#include "net/poller.hpp"

#if defined(__linux__)
#define MINIREDIS_USE_EPOLL 1
#include <sys/epoll.h>
#include <unordered_map>

namespace miniredis {

class EpollPoller : public Poller {
public:
    EpollPoller();
    ~EpollPoller() override;

    bool add_fd(int fd, uint32_t events, void* udata = nullptr) override;
    bool mod_fd(int fd, uint32_t events, void* udata = nullptr) override;
    bool del_fd(int fd) override;
    int poll(int timeout_ms, std::vector<FdEvent>& active_events) override;

private:
    struct EpollFdState {
        int fd{-1};
        uint32_t events{0};
        void* udata{nullptr};
    };

    int epoll_fd_{-1};
    std::vector<struct epoll_event> event_list_;
    std::unordered_map<int, EpollFdState> fd_states_;
};

} // namespace miniredis

#endif
