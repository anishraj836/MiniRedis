#include "protocol/command.hpp"

namespace miniredis {

Command::Command(std::vector<std::string> tokens)
    : tokens_(std::move(tokens)) {
    if (!tokens_.empty()) {
        upper_name_ = tokens_[0];
        std::transform(upper_name_.begin(), upper_name_.end(), upper_name_.begin(), [](unsigned char c) {
            return std::toupper(c);
        });
    }
}

} // namespace miniredis
