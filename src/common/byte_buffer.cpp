#include "common/byte_buffer.hpp"
#include <unistd.h>
#include <sys/uio.h>
#include <cerrno>

namespace miniredis {

ssize_t ByteBuffer::read_fd(int fd, int* saved_errno) {
    // Stack extra buffer (64KB) for scatter-gather read (readv)
    // This allows single system call reads even when write space in buffer is small
    char extrabuf[65536];
    struct iovec vec[2];
    const size_t writable = writable_bytes();

    vec[0].iov_base = begin_write();
    vec[0].iov_len = writable;
    vec[1].iov_base = extrabuf;
    vec[1].iov_len = sizeof(extrabuf);

    // If writable space is already large, only read into primary buffer
    const int iovcnt = (writable < sizeof(extrabuf)) ? 2 : 1;
    const ssize_t n = ::readv(fd, vec, iovcnt);

    if (n < 0) {
        if (saved_errno) *saved_errno = errno;
    } else if (static_cast<size_t>(n) <= writable) {
        write_index_ += n;
    } else {
        write_index_ = buffer_.size();
        append(extrabuf, n - writable);
    }
    return n;
}

ssize_t ByteBuffer::write_fd(int fd, int* saved_errno) {
    if (empty()) {
        return 0;
    }

    const ssize_t n = ::write(fd, peek(), readable_bytes());
    if (n < 0) {
        if (saved_errno) *saved_errno = errno;
    } else {
        retrieve(static_cast<size_t>(n));
    }
    return n;
}

} // namespace miniredis
