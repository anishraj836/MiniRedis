#pragma once

#include <cstdint>
#include <vector>
#include <memory>

namespace miniredis {

namespace EventMask {
    constexpr uint32_t NONE   = 0;
    constexpr uint32_t READ   = 1 << 0;
    constexpr uint32_t WRITE  = 1 << 1;
    constexpr uint32_t ERROR  = 1 << 2;
    constexpr uint32_t HUP    = 1 << 3;
}

struct FdEvent {
    int fd{-1};
    uint32_t events{0};
    void* udata{nullptr};
};

class Poller {
public:
    virtual ~Poller() = default;

    virtual bool add_fd(int fd, uint32_t events, void* udata = nullptr) = 0;
    virtual bool mod_fd(int fd, uint32_t events, void* udata = nullptr) = 0;
    virtual bool del_fd(int fd) = 0;
    virtual int poll(int timeout_ms, std::vector<FdEvent>& active_events) = 0;

    // Factory method that creates best poller for current OS
    static std::unique_ptr<Poller> create();
};

} // namespace miniredis
