#include "protocol/resp_parser.hpp"
#include <charconv>
#include <sstream>

namespace miniredis {

namespace {

bool parse_int64(std::string_view sv, int64_t& out_val) {
    if (sv.empty()) return false;
    const char* first = sv.data();
    const char* last = sv.data() + sv.size();
    auto [ptr, ec] = std::from_chars(first, last, out_val);
    return ec == std::errc() && ptr == last;
}

} // anonymous namespace

ParseResult RespParser::parse(ByteBuffer& buffer, RespValue& out_value) {
    if (buffer.empty()) {
        return ParseResult::INCOMPLETE;
    }
    return parse_internal(buffer, out_value);
}

ParseResult RespParser::parse_internal(ByteBuffer& buffer, RespValue& out_value) {
    if (buffer.empty()) {
        return ParseResult::INCOMPLETE;
    }

    char type_byte = *buffer.peek();
    switch (type_byte) {
        case '+': return parse_simple_string(buffer, out_value);
        case '-': return parse_error(buffer, out_value);
        case ':': return parse_integer(buffer, out_value);
        case '$': return parse_bulk_string(buffer, out_value);
        case '*': return parse_array(buffer, out_value);
        default:  return ParseResult::ERROR;
    }
}

ParseResult RespParser::parse_simple_string(ByteBuffer& buffer, RespValue& out_value) {
    const char* crlf = buffer.find_crlf();
    if (!crlf) return ParseResult::INCOMPLETE;

    const char* start = buffer.peek() + 1; // skip '+'
    size_t len = crlf - start;
    out_value = RespValue::make_simple_string(std::string(start, len));
    buffer.retrieve((crlf + 2) - buffer.peek());
    return ParseResult::OK;
}

ParseResult RespParser::parse_error(ByteBuffer& buffer, RespValue& out_value) {
    const char* crlf = buffer.find_crlf();
    if (!crlf) return ParseResult::INCOMPLETE;

    const char* start = buffer.peek() + 1; // skip '-'
    size_t len = crlf - start;
    out_value = RespValue::make_error(std::string(start, len));
    buffer.retrieve((crlf + 2) - buffer.peek());
    return ParseResult::OK;
}

ParseResult RespParser::parse_integer(ByteBuffer& buffer, RespValue& out_value) {
    const char* crlf = buffer.find_crlf();
    if (!crlf) return ParseResult::INCOMPLETE;

    const char* start = buffer.peek() + 1; // skip ':'
    std::string_view sv(start, crlf - start);
    int64_t val = 0;
    if (!parse_int64(sv, val)) {
        return ParseResult::ERROR;
    }
    out_value = RespValue::make_integer(val);
    buffer.retrieve((crlf + 2) - buffer.peek());
    return ParseResult::OK;
}

ParseResult RespParser::parse_bulk_string(ByteBuffer& buffer, RespValue& out_value) {
    const char* crlf = buffer.find_crlf();
    if (!crlf) return ParseResult::INCOMPLETE;

    const char* start = buffer.peek() + 1; // skip '$'
    std::string_view sv(start, crlf - start);
    int64_t len = 0;
    if (!parse_int64(sv, len)) {
        return ParseResult::ERROR;
    }

    if (len < -1) {
        return ParseResult::ERROR;
    }

    if (len == -1) {
        out_value = RespValue::make_null_bulk();
        buffer.retrieve((crlf + 2) - buffer.peek());
        return ParseResult::OK;
    }

    size_t header_len = (crlf + 2) - buffer.peek();
    size_t total_required = header_len + static_cast<size_t>(len) + 2; // +2 for trailing \r\n

    if (buffer.readable_bytes() < total_required) {
        return ParseResult::INCOMPLETE;
    }

    const char* data_start = buffer.peek() + header_len;
    out_value = RespValue::make_bulk_string(std::string(data_start, len));
    buffer.retrieve(total_required);
    return ParseResult::OK;
}

ParseResult RespParser::parse_array(ByteBuffer& buffer, RespValue& out_value) {
    size_t saved_index = buffer.reader_index();

    const char* crlf = buffer.find_crlf();
    if (!crlf) return ParseResult::INCOMPLETE;

    const char* start = buffer.peek() + 1; // skip '*'
    std::string_view sv(start, crlf - start);
    int64_t count = 0;
    if (!parse_int64(sv, count)) {
        return ParseResult::ERROR;
    }

    if (count < -1) {
        return ParseResult::ERROR;
    }

    if (count == -1) {
        out_value = RespValue::make_null_array();
        buffer.retrieve((crlf + 2) - buffer.peek());
        return ParseResult::OK;
    }

    if (count == 0) {
        out_value = RespValue::make_array({});
        buffer.retrieve((crlf + 2) - buffer.peek());
        return ParseResult::OK;
    }

    buffer.retrieve((crlf + 2) - buffer.peek());

    RespArray arr;
    arr.reserve(count);

    for (int64_t i = 0; i < count; ++i) {
        RespValue elem;
        ParseResult res = parse_internal(buffer, elem);
        if (res == ParseResult::INCOMPLETE) {
            buffer.set_reader_index(saved_index);
            return ParseResult::INCOMPLETE;
        } else if (res == ParseResult::ERROR) {
            return ParseResult::ERROR;
        }
        arr.push_back(std::move(elem));
    }

    out_value = RespValue::make_array(std::move(arr));
    return ParseResult::OK;
}

ParseResult RespParser::parse_inline_command(ByteBuffer& buffer, std::vector<std::string>& out_cmd) {
    const char* eol = buffer.find_eol();
    if (!eol) return ParseResult::INCOMPLETE;

    size_t line_len = eol - buffer.peek();
    if (line_len > 0 && *(eol - 1) == '\r') {
        line_len -= 1;
    }

    std::string_view line(buffer.peek(), line_len);
    buffer.retrieve((eol + 1) - buffer.peek());

    out_cmd.clear();
    std::string token;
    bool in_quotes = false;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (c == '\\' && in_quotes && i + 1 < line.size() && (line[i+1] == '"' || line[i+1] == '\\')) {
            token += line[++i];
        } else if (c == '"') {
            in_quotes = !in_quotes;
        } else if ((c == ' ' || c == '\t') && !in_quotes) {
            if (!token.empty()) {
                out_cmd.push_back(token);
                token.clear();
            }
        } else {
            token += c;
        }
    }
    if (!token.empty()) {
        out_cmd.push_back(token);
    }

    return out_cmd.empty() ? ParseResult::INCOMPLETE : ParseResult::OK;
}

ParseResult RespParser::parse_command(ByteBuffer& buffer, std::vector<std::string>& out_cmd) {
    if (buffer.empty()) return ParseResult::INCOMPLETE;

    // Check if input is standard RESP array (*...)
    if (*buffer.peek() == '*') {
        RespValue resp_val;
        ParseResult res = parse(buffer, resp_val);
        if (res != ParseResult::OK) {
            return res;
        }

        if (resp_val.type != RespType::ARRAY || resp_val.is_null) {
            return ParseResult::ERROR;
        }

        const auto& arr = resp_val.get_array();
        out_cmd.clear();
        out_cmd.reserve(arr.size());

        for (const auto& elem : arr) {
            if (elem.type == RespType::BULK_STRING && !elem.is_null) {
                out_cmd.push_back(elem.get_string());
            } else if (elem.type == RespType::SIMPLE_STRING) {
                out_cmd.push_back(elem.get_string());
            } else if (elem.type == RespType::INTEGER) {
                out_cmd.push_back(std::to_string(elem.get_integer()));
            } else {
                return ParseResult::ERROR;
            }
        }
        return out_cmd.empty() ? ParseResult::INCOMPLETE : ParseResult::OK;
    } else {
        // Inline command
        return parse_inline_command(buffer, out_cmd);
    }
}

} // namespace miniredis
