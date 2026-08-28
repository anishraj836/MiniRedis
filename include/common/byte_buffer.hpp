#pragma once

#include <vector>
#include <string>
#include <string_view>
#include <cstddef>
#include <cstring>
#include <algorithm>
#include <cstdint>

namespace miniredis {

/**
 * @brief High-performance contiguous buffer for non-blocking network I/O.
 *
 * Layout:
 * +-------------------+------------------+------------------+
 * | prependable bytes |  readable bytes  |  writable bytes  |
 * |                   |     (CONTENT)    |                  |
 * +-------------------+------------------+------------------+
 * |                   |                  |                  |
 * 0      <=      read_index_    <=   write_index_   <=    capacity
 */
class ByteBuffer {
public:
    static constexpr size_t DEFAULT_PREPEND_SIZE = 8;
    static constexpr size_t DEFAULT_INITIAL_SIZE = 16384; // 16KB

    explicit ByteBuffer(size_t initial_size = DEFAULT_INITIAL_SIZE, size_t prepend_size = DEFAULT_PREPEND_SIZE)
        : prepend_size_(prepend_size),
          read_index_(prepend_size),
          write_index_(prepend_size),
          buffer_(prepend_size + initial_size) {}

    ~ByteBuffer() = default;
    ByteBuffer(const ByteBuffer&) = default;
    ByteBuffer& operator=(const ByteBuffer&) = default;
    ByteBuffer(ByteBuffer&&) noexcept = default;
    ByteBuffer& operator=(ByteBuffer&&) noexcept = default;

    // Buffer state queries
    [[nodiscard]] size_t readable_bytes() const noexcept {
        return write_index_ - read_index_;
    }

    [[nodiscard]] size_t writable_bytes() const noexcept {
        return buffer_.size() - write_index_;
    }

    [[nodiscard]] size_t prependable_bytes() const noexcept {
        return read_index_;
    }

    [[nodiscard]] bool empty() const noexcept {
        return readable_bytes() == 0;
    }

    [[nodiscard]] size_t capacity() const noexcept {
        return buffer_.size();
    }

    [[nodiscard]] size_t reader_index() const noexcept {
        return read_index_;
    }

    void set_reader_index(size_t idx) noexcept {
        read_index_ = std::min(idx, write_index_);
    }

    // Pointers for direct memory access
    [[nodiscard]] const char* peek() const noexcept {
        return buffer_.data() + read_index_;
    }

    [[nodiscard]] char* begin_write() noexcept {
        return buffer_.data() + write_index_;
    }

    [[nodiscard]] const char* begin_write() const noexcept {
        return buffer_.data() + write_index_;
    }

    [[nodiscard]] std::string_view string_view() const noexcept {
        return std::string_view(peek(), readable_bytes());
    }

    // Consume bytes
    void retrieve(size_t len) noexcept {
        if (len >= readable_bytes()) {
            read_index_ = write_index_;
        } else {
            read_index_ += len;
        }
    }

    void retrieve_until(const char* end) noexcept {
        if (end >= peek() && end <= begin_write()) {
            retrieve(end - peek());
        }
    }

    void retrieve_all() noexcept {
        read_index_ = prepend_size_;
        write_index_ = prepend_size_;
    }

    std::string retrieve_as_string(size_t len) {
        size_t actual_len = std::min(len, readable_bytes());
        std::string result(peek(), actual_len);
        retrieve(actual_len);
        return result;
    }

    std::string retrieve_all_as_string() {
        return retrieve_as_string(readable_bytes());
    }

    // Appending data
    void ensure_writable_bytes(size_t len) {
        if (writable_bytes() < len) {
            make_space(len);
        }
    }

    void has_written(size_t len) noexcept {
        write_index_ += len;
    }

    void unwrite(size_t len) noexcept {
        if (len <= writable_bytes() + readable_bytes()) {
            write_index_ -= len;
        }
    }

    void append(const char* data, size_t len) {
        ensure_writable_bytes(len);
        std::memcpy(begin_write(), data, len);
        has_written(len);
    }

    void append(std::string_view sv) {
        append(sv.data(), sv.size());
    }

    void append(const void* data, size_t len) {
        append(static_cast<const char*>(data), len);
    }

    void append_char(char c) {
        ensure_writable_bytes(1);
        *begin_write() = c;
        has_written(1);
    }

    // Socket I/O
    ssize_t read_fd(int fd, int* saved_errno);
    ssize_t write_fd(int fd, int* saved_errno);

    // Search helpers
    [[nodiscard]] const char* find_crlf() const noexcept {
        const char* crlf = static_cast<const char*>(
            std::memchr(peek(), '\r', readable_bytes())
        );
        while (crlf != nullptr) {
            if (crlf + 1 < begin_write() && *(crlf + 1) == '\n') {
                return crlf;
            }
            size_t remaining = begin_write() - (crlf + 1);
            if (remaining == 0) break;
            crlf = static_cast<const char*>(std::memchr(crlf + 1, '\r', remaining));
        }
        return nullptr;
    }

    [[nodiscard]] const char* find_eol() const noexcept {
        return static_cast<const char*>(
            std::memchr(peek(), '\n', readable_bytes())
        );
    }

private:
    void make_space(size_t len) {
        if (read_index_ == write_index_) {
            read_index_ = prepend_size_;
            write_index_ = prepend_size_;
        }
        if (writable_bytes() + prependable_bytes() - prepend_size_ < len) {
            // Allocate more space
            buffer_.resize(write_index_ + len);
        } else {
            // Move readable data to front to consolidate space
            size_t readable = readable_bytes();
            std::memmove(buffer_.data() + prepend_size_, buffer_.data() + read_index_, readable);
            read_index_ = prepend_size_;
            write_index_ = read_index_ + readable;
        }
    }

    size_t prepend_size_;
    size_t read_index_;
    size_t write_index_;
    std::vector<char> buffer_;
};

} // namespace miniredis
