#pragma once

#include <string>
#include <string_view>
#include <iostream>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <sstream>

namespace miniredis {

enum class LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3,
    FATAL = 4
};

class Logger {
public:
    static Logger& instance() {
        static Logger instance;
        return instance;
    }

    void set_level(LogLevel level) noexcept {
        level_ = level;
    }

    [[nodiscard]] LogLevel level() const noexcept {
        return level_;
    }

    void log(LogLevel level, const char* file, int line, std::string_view msg);

private:
    Logger() : level_(LogLevel::INFO) {}
    ~Logger() = default;

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    LogLevel level_;
    std::mutex mutex_;
};

#define LOG_DEBUG(msg) miniredis::Logger::instance().log(miniredis::LogLevel::DEBUG, __FILE__, __LINE__, msg)
#define LOG_INFO(msg)  miniredis::Logger::instance().log(miniredis::LogLevel::INFO, __FILE__, __LINE__, msg)
#define LOG_WARN(msg)  miniredis::Logger::instance().log(miniredis::LogLevel::WARN, __FILE__, __LINE__, msg)
#define LOG_ERROR(msg) miniredis::Logger::instance().log(miniredis::LogLevel::ERROR, __FILE__, __LINE__, msg)
#define LOG_FATAL(msg) miniredis::Logger::instance().log(miniredis::LogLevel::FATAL, __FILE__, __LINE__, msg)

} // namespace miniredis
