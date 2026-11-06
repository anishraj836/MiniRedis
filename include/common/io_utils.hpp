#pragma once

#include <cerrno>
#include <cstddef>
#include <string>
#include <fcntl.h>
#include <unistd.h>

namespace miniredis {

// ::write() may write fewer bytes than asked (signals, pipes, nearly full
// disks). Keep writing until everything is out or a real error occurs.
inline bool write_all(int fd, const char* data, size_t len) noexcept {
    while (len > 0) {
        ssize_t n = ::write(fd, data, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

inline bool write_all(int fd, const std::string& data) noexcept {
    return write_all(fd, data.data(), data.size());
}

// Durably flush a file. On macOS plain fsync() does not flush the drive's
// write cache, so prefer F_FULLFSYNC and fall back to fsync() where it is not
// supported (e.g. some network filesystems).
inline bool fsync_fd(int fd) noexcept {
#if defined(__APPLE__)
    if (::fcntl(fd, F_FULLFSYNC) == 0) return true;
    return ::fsync(fd) == 0;
#else
    return ::fdatasync(fd) == 0;
#endif
}

// After rename(), the new directory entry itself must be made durable,
// otherwise a crash can roll the rename back.
inline void fsync_parent_dir(const std::string& path) noexcept {
    std::string dir = ".";
    auto slash = path.find_last_of('/');
    if (slash != std::string::npos) dir = slash == 0 ? "/" : path.substr(0, slash);
    int dfd = ::open(dir.c_str(), O_RDONLY);
    if (dfd >= 0) {
        ::fsync(dfd);
        ::close(dfd);
    }
}

} // namespace miniredis
