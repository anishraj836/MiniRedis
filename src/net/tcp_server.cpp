#include "net/tcp_server.hpp"
#include "common/logger.hpp"
#include <unistd.h>
#include <cerrno>

namespace miniredis {

TcpServer::TcpServer(EventLoop* loop, std::string host, uint16_t port, size_t num_threads)
    : main_loop_(loop),
      host_(std::move(host)),
      port_(port),
      thread_pool_(loop, num_threads) {}

TcpServer::~TcpServer() {
    stop();
}

bool TcpServer::start() {
    listen_socket_ = Socket::create_tcp();
    if (!listen_socket_.valid()) {
        return false;
    }

    listen_socket_.set_reuse_addr(true);
    listen_socket_.set_reuse_port(true);
    listen_socket_.set_nonblocking(true);

    if (!listen_socket_.bind(host_, port_)) {
        return false;
    }

    if (!listen_socket_.listen(1024)) {
        return false;
    }

    // Register listening socket on main event loop
    // In our event loop, we can wrap accept logic
    main_loop_->update_channel(listen_socket_.fd(), EventMask::READ, nullptr);

    // Custom task runner on main loop for socket events
    // We register a timer or direct polling hook
    main_loop_->run_every(1, [this]() {
        handle_accept();
    });

    thread_pool_.start();
    LOG_INFO("TcpServer listening on " + host_ + ":" + std::to_string(port_));
    return true;
}

void TcpServer::stop() {
    if (listen_socket_.valid()) {
        main_loop_->remove_channel(listen_socket_.fd());
        listen_socket_.close();
    }

    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        for (auto& [fd, conn] : connections_) {
            conn->get_loop()->run_in_loop([c = conn]() {
                c->connect_destroyed();
            });
        }
        connections_.clear();
    }

    thread_pool_.stop();
}

void TcpServer::handle_accept() {
    if (!listen_socket_.valid()) return;

    while (true) {
        struct sockaddr_in client_addr{};
        int client_fd = listen_socket_.accept(&client_addr);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                break;
            }
            break;
        }

        char ip_buf[INET_ADDRSTRLEN];
        ::inet_ntop(AF_INET, &client_addr.sin_addr, ip_buf, sizeof(ip_buf));
        std::string peer_addr = std::string(ip_buf) + ":" + std::to_string(ntohs(client_addr.sin_port));

        EventLoop* io_loop = thread_pool_.get_next_loop();
        auto conn = std::make_shared<Connection>(io_loop, client_fd, peer_addr);

        {
            std::lock_guard<std::mutex> lock(conn_mutex_);
            connections_[client_fd] = conn;
        }

        conn->set_message_callback(message_cb_);
        conn->set_write_complete_callback(write_complete_cb_);
        conn->set_close_callback([this](const ConnectionPtr& c) {
            if (close_cb_) {
                close_cb_(c);
            }
            remove_connection(c);  // drops the last owning reference -> socket is closed
        });

        if (conn_cb_) {
            conn_cb_(conn);
        }

        io_loop->run_in_loop([conn]() {
            conn->connect_established();
        });
    }
}

void TcpServer::remove_connection(const ConnectionPtr& conn) {
    main_loop_->run_in_loop([this, conn]() {
        remove_connection_in_loop(conn);
    });
}

void TcpServer::remove_connection_in_loop(const ConnectionPtr& conn) {
    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        connections_.erase(conn->fd());
    }
    EventLoop* io_loop = conn->get_loop();
    io_loop->queue_in_loop([conn]() {
        conn->connect_destroyed();
    });
}

} // namespace miniredis
