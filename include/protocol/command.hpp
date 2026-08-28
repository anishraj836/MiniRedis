#pragma once

#include <string>
#include <vector>
#include <string_view>
#include <algorithm>
#include <cstdint>

namespace miniredis {

enum class CommandFlag : uint32_t {
    NONE     = 0,
    READONLY = 1 << 0,
    WRITE    = 1 << 1,
    ADMIN    = 1 << 2,
    FAST     = 1 << 3
};

inline CommandFlag operator|(CommandFlag a, CommandFlag b) noexcept {
    return static_cast<CommandFlag>(static_cast<uint32_t>(a) | static_cast<uint32_t>(b));
}

inline bool operator&(CommandFlag a, CommandFlag b) noexcept {
    return (static_cast<uint32_t>(a) & static_cast<uint32_t>(b)) != 0;
}

class Command {
public:
    Command() = default;
    explicit Command(std::vector<std::string> tokens);

    [[nodiscard]] const std::string& name() const noexcept {
        static const std::string empty;
        return tokens_.empty() ? empty : tokens_[0];
    }

    [[nodiscard]] const std::string& upper_name() const noexcept {
        return upper_name_;
    }

    [[nodiscard]] size_t arg_count() const noexcept {
        return tokens_.size() > 0 ? tokens_.size() - 1 : 0;
    }

    [[nodiscard]] const std::string& arg(size_t index) const {
        return tokens_.at(index + 1);
    }

    [[nodiscard]] const std::vector<std::string>& tokens() const noexcept {
        return tokens_;
    }

    [[nodiscard]] bool empty() const noexcept {
        return tokens_.empty();
    }

private:
    std::vector<std::string> tokens_;
    std::string upper_name_;
};

} // namespace miniredis
