#include "net/connection.hpp"
#include "net/event_loop.hpp"
#include "common/logger.hpp"
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace miniredis {

Connection::Connection(EventLoop* loop, int fd, std::string peer_addr)
    : loop_(loop),
      socket_(fd),
      peer_addr_(std::move(peer_addr)),
      state_(ConnectionState::CONNECTING) {
    socket_.set_nonblocking(true);
    socket_.set_tcp_nodelay(true);
    socket_.set_keepalive(true);
}

Connection::~Connection() {
    LOG_DEBUG("Connection::~Connection() fd=" + std::to_string(socket_.fd()));
}

void Connection::connect_established() {
    loop_->assert_in_loop_thread();
    state_ = ConnectionState::CONNECTED;
    current_events_ = EventMask::READ;
    loop_->update_channel(socket_.fd(), current_events_, this);
}

void Connection::connect_destroyed() {
    loop_->assert_in_loop_thread();
    if (state_ == ConnectionState::CONNECTED || state_ == ConnectionState::DISCONNECTING) {
        state_ = ConnectionState::DISCONNECTED;
        loop_->remove_channel(socket_.fd());
        socket_.close();
    }
}

void Connection::handle_events(uint32_t events) {
    // Keep reference alive during event processing
    auto guard = shared_from_this();

    if (events & EventMask::ERROR) {
        handle_error();
        return;
    }

    if (events & EventMask::HUP && !(events & EventMask::READ)) {
        handle_close();
        return;
    }

    if (events & EventMask::READ) {
        handle_read();
    }

    if (events & EventMask::WRITE) {
        handle_write();
    }
}

void Connection::handle_read() {
    loop_->assert_in_loop_thread();
    int saved_errno = 0;
    ssize_t n = input_buffer_.read_fd(socket_.fd(), &saved_errno);

    if (n > 0) {
        if (state_ != ConnectionState::CONNECTED) {
            // Shutting down (after QUIT or a protocol error): replies can no
            // longer be sent, so input must not be executed either.
            input_buffer_.retrieve_all();
        } else if (message_cb_) {
            message_cb_(shared_from_this(), input_buffer_);
        }
    } else if (n == 0) {
        // Peer disconnected
        handle_close();
    } else {
        if (saved_errno != EAGAIN && saved_errno != EWOULDBLOCK && saved_errno != EINTR) {
            LOG_ERROR("Connection read error fd=" + std::to_string(socket_.fd()) + ": " + strerror(saved_errno));
            handle_error();
        }
    }
}

void Connection::handle_write() {
    loop_->assert_in_loop_thread();
    if (output_buffer_.empty()) {
        // Unregister WRITE interest to avoid spinning
        current_events_ &= ~EventMask::WRITE;
        loop_->update_channel(socket_.fd(), current_events_, this);
        return;
    }

    int saved_errno = 0;
    ssize_t n = output_buffer_.write_fd(socket_.fd(), &saved_errno);
    if (n > 0) {
        if (output_buffer_.empty()) {
            // All data sent: unregister WRITE interest
            current_events_ &= ~EventMask::WRITE;
            loop_->update_channel(socket_.fd(), current_events_, this);

            if (write_complete_cb_) {
                write_complete_cb_(shared_from_this());
            }

            if (state_ == ConnectionState::DISCONNECTING) {
                shutdown_in_loop();
            }
        }
    } else {
        if (saved_errno != EAGAIN && saved_errno != EWOULDBLOCK && saved_errno != EINTR) {
            LOG_ERROR("Connection write error fd=" + std::to_string(socket_.fd()) + ": " + strerror(saved_errno));
            handle_error();
        }
    }
}

void Connection::handle_close() {
    loop_->assert_in_loop_thread();
    if (state_ == ConnectionState::DISCONNECTED) return;

    state_ = ConnectionState::DISCONNECTED;
    loop_->remove_channel(socket_.fd());

    auto guard = shared_from_this();
    if (close_cb_) {
        close_cb_(guard);
    }
}

void Connection::handle_error() {
    int optval;
    socklen_t optlen = sizeof(optval);
    int err = 0;
    if (::getsockopt(socket_.fd(), SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0) {
        err = errno;
    } else {
        err = optval;
    }
    LOG_DEBUG("Connection error fd=" + std::to_string(socket_.fd()) + ": " + strerror(err));
    handle_close();
}

void Connection::send(const void* data, size_t len) {
    if (state_ != ConnectionState::CONNECTED) return;
    if (loop_->is_in_loop_thread()) {
        send_in_loop(data, len);
    } else {
        std::string str(static_cast<const char*>(data), len);
        loop_->queue_in_loop([guard = shared_from_this(), s = std::move(str)]() {
            guard->send_in_loop(s.data(), s.size());
        });
    }
}

void Connection::send(std::string_view sv) {
    send(sv.data(), sv.size());
}

void Connection::send(const std::string& str) {
    send(str.data(), str.size());
}

void Connection::send_in_loop(const void* data, size_t len) {
    loop_->assert_in_loop_thread();
    if (state_ != ConnectionState::CONNECTED) return;

    ssize_t nwrote = 0;
    size_t remaining = len;
    bool fault_error = false;

    // If output buffer is empty, attempt immediate direct write
    if (output_buffer_.empty()) {
        nwrote = ::write(socket_.fd(), data, len);
        if (nwrote >= 0) {
            remaining -= nwrote;
            if (remaining == 0 && write_complete_cb_) {
                write_complete_cb_(shared_from_this());
            }
        } else {
            nwrote = 0;
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                if (errno == EPIPE || errno == ECONNRESET) {
                    fault_error = true;
                }
            }
        }
    }

    if (fault_error) {
        handle_error();
        return;
    }

    // Buffer any remaining bytes and register for WRITE event
    if (remaining > 0) {
        output_buffer_.append(static_cast<const char*>(data) + nwrote, remaining);
        if (!(current_events_ & EventMask::WRITE)) {
            current_events_ |= EventMask::WRITE;
            loop_->update_channel(socket_.fd(), current_events_, this);
        }
    }
}

void Connection::shutdown() {
    if (state_ == ConnectionState::CONNECTED) {
        state_ = ConnectionState::DISCONNECTING;
        loop_->run_in_loop([guard = shared_from_this()]() {
            guard->shutdown_in_loop();
        });
    }
}

void Connection::shutdown_in_loop() {
    loop_->assert_in_loop_thread();
    if (output_buffer_.empty()) {
        ::shutdown(socket_.fd(), SHUT_WR);
    }
}

void Connection::force_close() {
    if (state_ == ConnectionState::CONNECTED || state_ == ConnectionState::DISCONNECTING) {
        loop_->queue_in_loop([guard = shared_from_this()]() {
            guard->handle_close();
        });
    }
}

} // namespace miniredis
