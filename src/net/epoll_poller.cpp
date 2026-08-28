#include "net/epoll_poller.hpp"

#ifdef MINIREDIS_USE_EPOLL
#include "common/logger.hpp"
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace miniredis {

EpollPoller::EpollPoller() : event_list_(256) {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        LOG_ERROR("Failed to create epoll instance: " + std::string(strerror(errno)));
    }
}

EpollPoller::~EpollPoller() {
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
    }
}

bool EpollPoller::add_fd(int fd, uint32_t events, void* udata) {
    if (epoll_fd_ < 0 || fd < 0) return false;

    fd_states_[fd] = {fd, events, udata};

    struct epoll_event ev{};
    if (events & EventMask::READ)  ev.events |= EPOLLIN | EPOLLRDHUP;
    if (events & EventMask::WRITE) ev.events |= EPOLLOUT;
    ev.data.ptr = &fd_states_[fd];

    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        LOG_ERROR("epoll_ctl ADD failed on fd " + std::to_string(fd) + ": " + strerror(errno));
        fd_states_.erase(fd);
        return false;
    }
    return true;
}

bool EpollPoller::mod_fd(int fd, uint32_t events, void* udata) {
    if (epoll_fd_ < 0 || fd < 0) return false;

    auto it = fd_states_.find(fd);
    if (it == fd_states_.end()) {
        return add_fd(fd, events, udata);
    }

    it->second = {fd, events, udata};

    struct epoll_event ev{};
    if (events & EventMask::READ)  ev.events |= EPOLLIN | EPOLLRDHUP;
    if (events & EventMask::WRITE) ev.events |= EPOLLOUT;
    ev.data.ptr = &it->second;

    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        LOG_ERROR("epoll_ctl MOD failed on fd " + std::to_string(fd) + ": " + strerror(errno));
        return false;
    }
    return true;
}

bool EpollPoller::del_fd(int fd) {
    if (epoll_fd_ < 0 || fd < 0) return false;

    struct epoll_event ev{};
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, &ev);
    fd_states_.erase(fd);
    return true;
}

int EpollPoller::poll(int timeout_ms, std::vector<FdEvent>& active_events) {
    active_events.clear();
    if (epoll_fd_ < 0) return -1;

    int nevents = ::epoll_wait(epoll_fd_, event_list_.data(), static_cast<int>(event_list_.size()), timeout_ms);
    if (nevents < 0) {
        if (errno == EINTR) return 0;
        LOG_ERROR("epoll_wait failed: " + std::string(strerror(errno)));
        return -1;
    }

    for (int i = 0; i < nevents; ++i) {
        const auto& ev = event_list_[i];
        auto* state = static_cast<EpollFdState*>(ev.data.ptr);
        if (!state) continue;

        uint32_t events = 0;
        if (ev.events & (EPOLLIN | EPOLLPRI))   events |= EventMask::READ;
        if (ev.events & EPOLLOUT)               events |= EventMask::WRITE;
        if (ev.events & (EPOLLERR))             events |= EventMask::ERROR;
        if (ev.events & (EPOLLHUP | EPOLLRDHUP)) events |= EventMask::HUP;

        active_events.push_back({state->fd, events, state->udata});
    }

    if (nevents == static_cast<int>(event_list_.size())) {
        event_list_.resize(event_list_.size() * 2);
    }
    return nevents;
}

} // namespace miniredis

#endif
