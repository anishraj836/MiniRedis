#include "net/kqueue_poller.hpp"

#ifdef MINIREDIS_USE_KQUEUE
#include "common/logger.hpp"
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace miniredis {

KqueuePoller::KqueuePoller() : event_list_(256) {
    kq_fd_ = ::kqueue();
    if (kq_fd_ < 0) {
        LOG_ERROR("Failed to create kqueue: " + std::string(strerror(errno)));
    }
}

KqueuePoller::~KqueuePoller() {
    if (kq_fd_ >= 0) {
        ::close(kq_fd_);
    }
}

bool KqueuePoller::add_fd(int fd, uint32_t events, void* udata) {
    if (kq_fd_ < 0 || fd < 0) return false;

    struct kevent changes[2];
    int nchanges = 0;

    if (events & EventMask::READ) {
        EV_SET(&changes[nchanges++], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, udata);
    }
    if (events & EventMask::WRITE) {
        EV_SET(&changes[nchanges++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, udata);
    }

    if (nchanges > 0) {
        if (::kevent(kq_fd_, changes, nchanges, nullptr, 0, nullptr) < 0) {
            LOG_ERROR("Kqueue add_fd failed on fd " + std::to_string(fd) + ": " + strerror(errno));
            return false;
        }
    }
    fd_states_[fd] = {events, udata};
    return true;
}

bool KqueuePoller::mod_fd(int fd, uint32_t events, void* udata) {
    auto it = fd_states_.find(fd);
    if (it == fd_states_.end()) {
        return add_fd(fd, events, udata);
    }

    uint32_t old_events = it->second.events;
    struct kevent changes[4];
    int nchanges = 0;

    // READ filter adjustment
    if ((old_events & EventMask::READ) && !(events & EventMask::READ)) {
        EV_SET(&changes[nchanges++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    } else if (!(old_events & EventMask::READ) && (events & EventMask::READ)) {
        EV_SET(&changes[nchanges++], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, udata);
    }

    // WRITE filter adjustment
    if ((old_events & EventMask::WRITE) && !(events & EventMask::WRITE)) {
        EV_SET(&changes[nchanges++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    } else if (!(old_events & EventMask::WRITE) && (events & EventMask::WRITE)) {
        EV_SET(&changes[nchanges++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, udata);
    }

    if (nchanges > 0) {
        if (::kevent(kq_fd_, changes, nchanges, nullptr, 0, nullptr) < 0) {
            LOG_ERROR("Kqueue mod_fd failed on fd " + std::to_string(fd) + ": " + strerror(errno));
            return false;
        }
    }
    fd_states_[fd] = {events, udata};
    return true;
}

bool KqueuePoller::del_fd(int fd) {
    auto it = fd_states_.find(fd);
    if (it == fd_states_.end()) return true;

    uint32_t old_events = it->second.events;
    struct kevent changes[2];
    int nchanges = 0;

    if (old_events & EventMask::READ) {
        EV_SET(&changes[nchanges++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    }
    if (old_events & EventMask::WRITE) {
        EV_SET(&changes[nchanges++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    }

    if (nchanges > 0) {
        ::kevent(kq_fd_, changes, nchanges, nullptr, 0, nullptr);
    }
    fd_states_.erase(it);
    return true;
}

int KqueuePoller::poll(int timeout_ms, std::vector<FdEvent>& active_events) {
    active_events.clear();
    if (kq_fd_ < 0) return -1;

    struct timespec ts{};
    struct timespec* pts = nullptr;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000;
        pts = &ts;
    }

    int nevents = ::kevent(kq_fd_, nullptr, 0, event_list_.data(), static_cast<int>(event_list_.size()), pts);
    if (nevents < 0) {
        if (errno == EINTR) return 0;
        LOG_ERROR("kevent wait failed: " + std::string(strerror(errno)));
        return -1;
    }

    for (int i = 0; i < nevents; ++i) {
        const auto& ev = event_list_[i];
        int fd = static_cast<int>(ev.ident);
        uint32_t events = 0;

        if (ev.filter == EVFILT_READ) {
            events |= EventMask::READ;
        } else if (ev.filter == EVFILT_WRITE) {
            events |= EventMask::WRITE;
        }

        if (ev.flags & EV_ERROR) {
            events |= EventMask::ERROR;
        }
        if (ev.flags & EV_EOF) {
            events |= EventMask::HUP;
        }

        active_events.push_back({fd, events, ev.udata});
    }

    // Grow event list buffer dynamically if saturated
    if (nevents == static_cast<int>(event_list_.size())) {
        event_list_.resize(event_list_.size() * 2);
    }

    return nevents;
}

} // namespace miniredis

#endif
