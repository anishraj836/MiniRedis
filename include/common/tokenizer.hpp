#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace miniredis {

// Splits a command line into tokens, following redis-cli's rules:
//  - Tokens are separated by spaces or tabs.
//  - Single or double quotes group text, including spaces.
//    Inside a quoted section the other quote character is literal,
//    and \" \' \\ are escapes for the quote and backslash.
//  - A closing quote must be followed by a space or the end of the line.
// Returns false and sets `error` when the line is malformed.
inline bool tokenize_command_line(std::string_view line,
                                  std::vector<std::string>& out,
                                  std::string& error) {
    out.clear();
    std::string token;
    bool has_token = false;  // true once a token has started, so "" counts as an argument
    const std::size_t n = line.size();
    std::size_t i = 0;

    while (i < n) {
        const char c = line[i];

        if (c == ' ' || c == '\t') {
            if (has_token) {
                out.push_back(std::move(token));
                token.clear();
                has_token = false;
            }
            ++i;
            continue;
        }

        if (c == '"' || c == '\'') {
            const char quote = c;
            has_token = true;
            ++i;
            bool closed = false;

            while (i < n) {
                const char d = line[i];
                if (d == '\\' && i + 1 < n && (line[i + 1] == quote || line[i + 1] == '\\')) {
                    token += line[i + 1];
                    i += 2;
                } else if (d == quote) {
                    closed = true;
                    ++i;
                    break;
                } else {
                    token += d;
                    ++i;
                }
            }

            if (!closed) {
                error = "unbalanced quotes in request";
                return false;
            }
            if (i < n && line[i] != ' ' && line[i] != '\t') {
                error = "closing quote must be followed by a space";
                return false;
            }
            continue;
        }

        token += c;
        has_token = true;
        ++i;
    }

    if (has_token) {
        out.push_back(std::move(token));
    }
    return true;
}

}  // namespace miniredis
