#include "server/command_rewrite.hpp"
#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>

namespace miniredis {

namespace {

std::string upper(const std::string& s) {
    std::string out = s;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return std::toupper(c); });
    return out;
}

bool parse_positive(const std::string& s, int64_t& out) {
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
}

// deadline = amount * unit + base, rejecting overflow.
bool deadline(int64_t amount, int64_t unit, int64_t base, int64_t& out) {
    int64_t product = 0;
    if (__builtin_mul_overflow(amount, unit, &product)) return false;
    return !__builtin_add_overflow(product, base, &out);
}

} // anonymous namespace

std::optional<std::vector<std::string>> normalize_for_propagation(const std::vector<std::string>& tokens,
                                                                  int64_t now) {
    if (tokens.empty()) return std::nullopt;
    const std::string name = upper(tokens[0]);

    if ((name == "EXPIRE" || name == "PEXPIRE" || name == "EXPIREAT") && tokens.size() == 3) {
        int64_t amount = 0;
        int64_t at = 0;
        if (!parse_positive(tokens[2], amount)) return std::nullopt;
        const int64_t unit = (name == "PEXPIRE") ? 1 : 1000;
        const int64_t base = (name == "EXPIREAT") ? 0 : now;
        if (!deadline(amount, unit, base, at)) return std::nullopt;
        if (at < 0) at = 0; // a deadline in the past means "expire now"
        return std::vector<std::string>{"PEXPIREAT", tokens[1], std::to_string(at)};
    }

    if (name == "SETEX" && tokens.size() == 4) {
        int64_t sec = 0;
        int64_t at = 0;
        if (!parse_positive(tokens[2], sec) || sec <= 0 || !deadline(sec, 1000, now, at)) return std::nullopt;
        return std::vector<std::string>{"SET", tokens[1], tokens[3], "PXAT", std::to_string(at)};
    }

    if (name == "SET" && tokens.size() > 3) {
        std::vector<std::string> out(tokens.begin(), tokens.begin() + 3);
        bool changed = false;
        for (size_t i = 3; i < tokens.size(); ++i) {
            const std::string opt = upper(tokens[i]);
            if ((opt == "EX" || opt == "PX" || opt == "EXAT") && i + 1 < tokens.size()) {
                int64_t amount = 0;
                int64_t at = 0;
                if (!parse_positive(tokens[i + 1], amount) || amount <= 0) return std::nullopt;
                const int64_t unit = (opt == "PX") ? 1 : 1000;
                const int64_t base = (opt == "EXAT") ? 0 : now;
                if (!deadline(amount, unit, base, at)) return std::nullopt;
                out.push_back("PXAT");
                out.push_back(std::to_string(at));
                changed = true;
                ++i;
            } else {
                out.push_back(tokens[i]);
            }
        }
        if (changed) return out;
    }

    return std::nullopt;
}

} // namespace miniredis
