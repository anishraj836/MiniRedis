#include "net/poll_poller.hpp"
#include "common/logger.hpp"
#include <cerrno>
#include <cstring>

namespace miniredis {

PollPoller::PollPoller() = default;

bool PollPoller::add_fd(int fd, uint32_t events, void* udata) {
    if (fd < 0 || fd_map_.find(fd) != fd_map_.end()) return false;

    short poll_events = 0;
    if (events & EventMask::READ)  poll_events |= POLLIN | POLLPRI;
    if (events & EventMask::WRITE) poll_events |= POLLOUT;

    struct pollfd pfd{};
    pfd.fd = fd;
    pfd.events = poll_events;
    pfd.revents = 0;

    size_t idx = poll_fds_.size();
    poll_fds_.push_back(pfd);
    fd_map_[fd] = {events, udata, idx};
    return true;
}

bool PollPoller::mod_fd(int fd, uint32_t events, void* udata) {
    auto it = fd_map_.find(fd);
    if (it == fd_map_.end()) {
        return add_fd(fd, events, udata);
    }

    short poll_events = 0;
    if (events & EventMask::READ)  poll_events |= POLLIN | POLLPRI;
    if (events & EventMask::WRITE) poll_events |= POLLOUT;

    size_t idx = it->second.poll_index;
    poll_fds_[idx].events = poll_events;
    it->second.events = events;
    it->second.udata = udata;
    return true;
}

bool PollPoller::del_fd(int fd) {
    auto it = fd_map_.find(fd);
    if (it == fd_map_.end()) return false;

    size_t idx = it->second.poll_index;
    size_t last_idx = poll_fds_.size() - 1;

    if (idx != last_idx) {
        struct pollfd last_pfd = poll_fds_.back();
        poll_fds_[idx] = last_pfd;
        fd_map_[last_pfd.fd].poll_index = idx;
    }
    poll_fds_.pop_back();
    fd_map_.erase(it);
    return true;
}

int PollPoller::poll(int timeout_ms, std::vector<FdEvent>& active_events) {
    active_events.clear();
    if (poll_fds_.empty()) return 0;

    int nready = ::poll(poll_fds_.data(), poll_fds_.size(), timeout_ms);
    if (nready < 0) {
        if (errno == EINTR) return 0;
        LOG_ERROR("POSIX poll failed: " + std::string(strerror(errno)));
        return -1;
    }

    for (size_t i = 0; i < poll_fds_.size() && nready > 0; ++i) {
        const auto& pfd = poll_fds_[i];
        if (pfd.revents == 0) continue;

        auto it = fd_map_.find(pfd.fd);
        if (it == fd_map_.end()) continue;

        uint32_t events = 0;
        if (pfd.revents & (POLLIN | POLLPRI)) events |= EventMask::READ;
        if (pfd.revents & POLLOUT)            events |= EventMask::WRITE;
        if (pfd.revents & POLLERR)            events |= EventMask::ERROR;
        if (pfd.revents & (POLLHUP | POLLNVAL)) events |= EventMask::HUP;

        active_events.push_back({pfd.fd, events, it->second.udata});
        --nready;
    }

    return static_cast<int>(active_events.size());
}

} // namespace miniredis
