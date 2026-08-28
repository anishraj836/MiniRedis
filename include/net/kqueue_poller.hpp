#pragma once

#include "net/poller.hpp"

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#define MINIREDIS_USE_KQUEUE 1
#include <sys/event.h>
#include <sys/time.h>
#include <unordered_map>

namespace miniredis {

class KqueuePoller : public Poller {
public:
    KqueuePoller();
    ~KqueuePoller() override;

    bool add_fd(int fd, uint32_t events, void* udata = nullptr) override;
    bool mod_fd(int fd, uint32_t events, void* udata = nullptr) override;
    bool del_fd(int fd) override;
    int poll(int timeout_ms, std::vector<FdEvent>& active_events) override;

private:
    int kq_fd_{-1};
    std::vector<struct kevent> event_list_;
    struct FdState {
        uint32_t events{0};
        void* udata{nullptr};
    };
    std::unordered_map<int, FdState> fd_states_;
};

} // namespace miniredis

#endif
