#pragma once

#include "net/socket.hpp"
#include "common/byte_buffer.hpp"
#include <memory>
#include <functional>
#include <string>
#include <string_view>
#include <atomic>

namespace miniredis {

class EventLoop;
class Connection;

using ConnectionPtr = std::shared_ptr<Connection>;
using MessageCallback = std::function<void(const ConnectionPtr&, ByteBuffer&)>;
using CloseCallback = std::function<void(const ConnectionPtr&)>;
using WriteCompleteCallback = std::function<void(const ConnectionPtr&)>;

enum class ConnectionState {
    CONNECTING,
    CONNECTED,
    DISCONNECTING,
    DISCONNECTED
};

class Connection : public std::enable_shared_from_this<Connection> {
public:
    Connection(EventLoop* loop, int fd, std::string peer_addr);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    [[nodiscard]] int fd() const noexcept { return socket_.fd(); }
    [[nodiscard]] EventLoop* get_loop() const noexcept { return loop_; }
    [[nodiscard]] const std::string& peer_address() const noexcept { return peer_addr_; }
    [[nodiscard]] ConnectionState state() const noexcept { return state_; }
    [[nodiscard]] bool is_connected() const noexcept { return state_ == ConnectionState::CONNECTED; }

    // Callback registrations
    void set_message_callback(MessageCallback cb) { message_cb_ = std::move(cb); }
    void set_close_callback(CloseCallback cb) { close_cb_ = std::move(cb); }
    void set_write_complete_callback(WriteCompleteCallback cb) { write_complete_cb_ = std::move(cb); }

    // Lifecycle
    void connect_established();
    void connect_destroyed();

    // Data transmission
    void send(const void* data, size_t len);
    void send(std::string_view sv);
    void send(const std::string& str);
    void shutdown();
    void force_close();

    // Event handler invoked by EventLoop
    void handle_events(uint32_t events);

    // Context / Client storage
    void set_context(void* ctx) noexcept { context_ = ctx; }
    [[nodiscard]] void* get_context() const noexcept { return context_; }

private:
    void handle_read();
    void handle_write();
    void handle_close();
    void handle_error();

    void send_in_loop(const void* data, size_t len);
    void shutdown_in_loop();

    EventLoop* loop_;
    Socket socket_;
    std::string peer_addr_;
    std::atomic<ConnectionState> state_{ConnectionState::CONNECTING};

    ByteBuffer input_buffer_;
    ByteBuffer output_buffer_;

    uint32_t current_events_{0};

    MessageCallback message_cb_;
    CloseCallback close_cb_;
    WriteCompleteCallback write_complete_cb_;

    void* context_{nullptr};
};

} // namespace miniredis
