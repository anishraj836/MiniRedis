#pragma once

#include "net/event_loop.hpp"
#include "net/socket.hpp"
#include "net/connection.hpp"
#include "net/thread_pool.hpp"
#include <unordered_map>
#include <memory>
#include <string>
#include <mutex>

namespace miniredis {

using NewConnectionCallback = std::function<void(const ConnectionPtr&)>;

class TcpServer {
public:
    TcpServer(EventLoop* loop, std::string host, uint16_t port, size_t num_threads = 4);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void set_connection_callback(NewConnectionCallback cb) { conn_cb_ = std::move(cb); }
    void set_message_callback(MessageCallback cb) { message_cb_ = std::move(cb); }
    void set_write_complete_callback(WriteCompleteCallback cb) { write_complete_cb_ = std::move(cb); }
    // Invoked when a connection closes, before the server releases it.
    // Use this instead of Connection::set_close_callback: the server installs
    // its own close hook on every connection to free the socket.
    void set_close_callback(CloseCallback cb) { close_cb_ = std::move(cb); }

    bool start();
    void stop();

    [[nodiscard]] EventLoop* get_main_loop() const noexcept { return main_loop_; }
    [[nodiscard]] ThreadPool* get_thread_pool() noexcept { return &thread_pool_; }

private:
    void handle_accept();
    void remove_connection(const ConnectionPtr& conn);
    void remove_connection_in_loop(const ConnectionPtr& conn);

    EventLoop* main_loop_;
    std::string host_;
    uint16_t port_;
    Socket listen_socket_;
    ThreadPool thread_pool_;

    std::mutex conn_mutex_;
    std::unordered_map<int, ConnectionPtr> connections_;

    NewConnectionCallback conn_cb_;
    MessageCallback message_cb_;
    WriteCompleteCallback write_complete_cb_;
    CloseCallback close_cb_;
};

} // namespace miniredis
