#include "common/logger.hpp"
#include <ctime>

namespace miniredis {

namespace {

const char* level_to_color(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "\033[36m"; // Cyan
        case LogLevel::INFO:  return "\033[32m"; // Green
        case LogLevel::WARN:  return "\033[33m"; // Yellow
        case LogLevel::ERROR: return "\033[31m"; // Red
        case LogLevel::FATAL: return "\033[35m"; // Magenta
    }
    return "\033[0m";
}

const char* level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
    }
    return "UNKNOWN";
}

const char* extract_filename(const char* path) {
    const char* last_slash = path;
    for (const char* p = path; *p != '\0'; ++p) {
        if (*p == '/' || *p == '\\') {
            last_slash = p + 1;
        }
    }
    return last_slash;
}

} // anonymous namespace

void Logger::log(LogLevel level, const char* file, int line, std::string_view msg) {
    if (level < level_) {
        return;
    }

    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()
    ).count() % 1000;
    std::time_t tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf{};
#if defined(_WIN32)
    localtime_s(&tm_buf, &tt);
#else
    localtime_r(&tt, &tm_buf);
#endif

    char time_str[32];
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_buf);

    std::lock_guard<std::mutex> lock(mutex_);
    std::cout << level_to_color(level)
              << "[" << time_str << "." << std::setfill('0') << std::setw(3) << ms << "]"
              << " [" << level_to_string(level) << "]"
              << " [" << extract_filename(file) << ":" << line << "] "
              << msg
              << "\033[0m\n"
              << std::flush;
}

} // namespace miniredis
