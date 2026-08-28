#include "net/socket.hpp"
#include "common/logger.hpp"
#include <cstring>
#include <cerrno>

namespace miniredis {

Socket Socket::create_tcp() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        LOG_ERROR("Failed to create TCP socket: " + std::string(strerror(errno)));
        return Socket(-1);
    }
    return Socket(fd);
}

bool Socket::set_nonblocking(bool on) {
    if (fd_ < 0) return false;
    int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags < 0) return false;
    if (on) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    return ::fcntl(fd_, F_SETFL, flags) >= 0;
}

bool Socket::set_reuse_addr(bool on) {
    if (fd_ < 0) return false;
    int opt = on ? 1 : 0;
    return ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == 0;
}

bool Socket::set_reuse_port(bool on) {
#ifdef SO_REUSEPORT
    if (fd_ < 0) return false;
    int opt = on ? 1 : 0;
    return ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) == 0;
#else
    (void)on;
    return true;
#endif
}

bool Socket::set_tcp_nodelay(bool on) {
    if (fd_ < 0) return false;
    int opt = on ? 1 : 0;
    return ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == 0;
}

bool Socket::set_keepalive(bool on) {
    if (fd_ < 0) return false;
    int opt = on ? 1 : 0;
    return ::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) == 0;
}

bool Socket::set_send_buffer_size(int size) {
    if (fd_ < 0) return false;
    return ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) == 0;
}

bool Socket::set_recv_buffer_size(int size) {
    if (fd_ < 0) return false;
    return ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) == 0;
}

bool Socket::bind(const std::string& host, uint16_t port) {
    if (fd_ < 0) return false;

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host.empty() || host == "0.0.0.0") {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else {
        if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
            LOG_ERROR("Invalid IP address for bind: " + host);
            return false;
        }
    }

    if (::bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        LOG_ERROR("Failed to bind socket to " + host + ":" + std::to_string(port) + " : " + strerror(errno));
        return false;
    }
    return true;
}

bool Socket::listen(int backlog) {
    if (fd_ < 0) return false;
    if (::listen(fd_, backlog) < 0) {
        LOG_ERROR("Failed to listen on socket: " + std::string(strerror(errno)));
        return false;
    }
    return true;
}

int Socket::accept(struct sockaddr_in* client_addr) {
    if (fd_ < 0) return -1;
    struct sockaddr_in addr{};
    socklen_t addr_len = sizeof(addr);
    
    int client_fd = ::accept(fd_, reinterpret_cast<struct sockaddr*>(&addr), &addr_len);
    if (client_fd >= 0 && client_addr) {
        *client_addr = addr;
    }
    return client_fd;
}

bool Socket::connect(const std::string& host, uint16_t port) {
    if (fd_ < 0) return false;

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        LOG_ERROR("Invalid host address for connect: " + host);
        return false;
    }

    if (::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        return false;
    }
    return true;
}

std::string Socket::get_peer_ip() const {
    if (fd_ < 0) return "";
    struct sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (::getpeername(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
        return "";
    }
    char ip_buf[INET_ADDRSTRLEN];
    if (::inet_ntop(AF_INET, &addr.sin_addr, ip_buf, sizeof(ip_buf)) == nullptr) {
        return "";
    }
    return std::string(ip_buf);
}

uint16_t Socket::get_peer_port() const {
    if (fd_ < 0) return 0;
    struct sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (::getpeername(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
        return 0;
    }
    return ntohs(addr.sin_port);
}

std::string Socket::get_peer_address() const {
    return get_peer_ip() + ":" + std::to_string(get_peer_port());
}

} // namespace miniredis
