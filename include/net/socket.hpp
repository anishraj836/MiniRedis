#pragma once

#include <string>
#include <cstdint>
#include <string_view>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/tcp.h>

namespace miniredis {

/**
 * @brief RAII wrapper around POSIX non-blocking socket descriptor.
 */
class Socket {
public:
    explicit Socket(int fd = -1) noexcept : fd_(fd) {}
    ~Socket() noexcept {
        close();
    }

    // Move-only semantics
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }

    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    [[nodiscard]] int fd() const noexcept { return fd_; }
    [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

    int release() noexcept {
        int temp = fd_;
        fd_ = -1;
        return temp;
    }

    void reset(int fd = -1) noexcept {
        if (fd_ != fd) {
            close();
            fd_ = fd;
        }
    }

    void close() noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    // Factory methods
    static Socket create_tcp();

    // Socket configuration options
    bool set_nonblocking(bool on = true);
    bool set_reuse_addr(bool on = true);
    bool set_reuse_port(bool on = true);
    bool set_tcp_nodelay(bool on = true);
    bool set_keepalive(bool on = true);
    bool set_send_buffer_size(int size);
    bool set_recv_buffer_size(int size);

    // Server operations
    bool bind(const std::string& host, uint16_t port);
    bool listen(int backlog = 1024);
    int accept(struct sockaddr_in* client_addr = nullptr);

    // Client operations
    bool connect(const std::string& host, uint16_t port);

    // Address utilities
    [[nodiscard]] std::string get_peer_ip() const;
    [[nodiscard]] uint16_t get_peer_port() const;
    [[nodiscard]] std::string get_peer_address() const;

private:
    int fd_{-1};
};

} // namespace miniredis
