#pragma once

#include "protocol/resp_types.hpp"
#include "protocol/resp_parser.hpp"
#include "protocol/resp_serializer.hpp"
#include "net/socket.hpp"
#include "common/byte_buffer.hpp"
#include <string>
#include <vector>
#include <optional>

namespace miniredis {

class RedisClient {
public:
    RedisClient() = default;
    ~RedisClient();

    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    bool connect(const std::string& host, uint16_t port);
    void disconnect();
    [[nodiscard]] bool is_connected() const noexcept { return socket_.valid(); }

    // Synchronous single command
    RespValue execute(const std::vector<std::string>& args);

    // Send command without waiting for immediate response (for pipelining)
    bool send_command(const std::vector<std::string>& args);

    // Read single response (for pipelining)
    RespValue read_response();

    // Raw command string execution
    RespValue execute_line(const std::string& line);

private:
    Socket socket_;
    ByteBuffer input_buffer_;
    RespParser parser_;
};

} // namespace miniredis
