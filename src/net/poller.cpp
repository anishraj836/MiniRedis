#include "net/poller.hpp"
#include "net/kqueue_poller.hpp"
#include "net/epoll_poller.hpp"
#include "net/poll_poller.hpp"

namespace miniredis {

std::unique_ptr<Poller> Poller::create() {
#if defined(MINIREDIS_USE_KQUEUE)
    return std::make_unique<KqueuePoller>();
#elif defined(MINIREDIS_USE_EPOLL)
    return std::make_unique<EpollPoller>();
#else
    return std::make_unique<PollPoller>();
#endif
}

} // namespace miniredis
