#include "client/redis_client.hpp"
#include <unistd.h>
#include <poll.h>
#include <sstream>
#include <cerrno>

namespace miniredis {

RedisClient::~RedisClient() {
    disconnect();
}

bool RedisClient::connect(const std::string& host, uint16_t port) {
    socket_ = Socket::create_tcp();
    if (!socket_.valid()) return false;

    socket_.set_tcp_nodelay(true);
    if (!socket_.connect(host, port)) {
        socket_.close();
        return false;
    }
    return true;
}

void RedisClient::disconnect() {
    socket_.close();
    input_buffer_.retrieve_all();
}

bool RedisClient::send_command(const std::vector<std::string>& args) {
    if (!socket_.valid()) return false;
    std::string payload = RespSerializer::serialize_string_array(args);
    
    size_t total_sent = 0;
    while (total_sent < payload.size()) {
        ssize_t n = ::write(socket_.fd(), payload.data() + total_sent, payload.size() - total_sent);
        if (n > 0) {
            total_sent += n;
        } else if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd{};
                pfd.fd = socket_.fd();
                pfd.events = POLLOUT;
                ::poll(&pfd, 1, 1000);
                continue;
            }
            disconnect();
            return false;
        } else {
            disconnect();
            return false;
        }
    }
    return true;
}

RespValue RedisClient::read_response() {
    if (!socket_.valid()) {
        return RespValue::make_error("ERR not connected");
    }

    RespValue resp;
    while (true) {
        ParseResult res = parser_.parse(input_buffer_, resp);
        if (res == ParseResult::OK) {
            return resp;
        } else if (res == ParseResult::INCOMPLETE) {
            struct pollfd pfd{};
            pfd.fd = socket_.fd();
            pfd.events = POLLIN;
            int pret = ::poll(&pfd, 1, 5000); // 5s timeout
            if (pret <= 0) {
                if (pret == 0) return RespValue::make_error("ERR timeout reading from server");
                if (errno == EINTR) continue;
                disconnect();
                return RespValue::make_error("ERR poll error reading from server");
            }

            int err = 0;
            ssize_t n = input_buffer_.read_fd(socket_.fd(), &err);
            if (n > 0) {
                continue;
            } else if (n == 0) {
                disconnect();
                return RespValue::make_error("ERR connection closed by server");
            } else {
                if (err == EINTR || err == EAGAIN || err == EWOULDBLOCK) continue;
                disconnect();
                return RespValue::make_error("ERR read failure from server");
            }
        } else {
            input_buffer_.retrieve_all();
            return RespValue::make_error("ERR protocol decoding error");
        }
    }
}

RespValue RedisClient::execute(const std::vector<std::string>& args) {
    if (!send_command(args)) {
        return RespValue::make_error("ERR failed to send command");
    }
    return read_response();
}

RespValue RedisClient::execute_line(const std::string& line) {
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    if (tokens.empty()) {
        return RespValue::make_error("ERR empty line");
    }
    return execute(tokens);
}

} // namespace miniredis
