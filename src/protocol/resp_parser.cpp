#include "protocol/resp_parser.hpp"
#include "common/tokenizer.hpp"
#include <charconv>
#include <cstring>
#include <utility>

namespace miniredis {

namespace {

bool parse_int64(std::string_view sv, int64_t& out_val) {
    if (sv.empty()) return false;
    const char* first = sv.data();
    const char* last = sv.data() + sv.size();
    auto [ptr, ec] = std::from_chars(first, last, out_val);
    return ec == std::errc() && ptr == last;
}

// Finds "\r\n" in [data + pos, data + len). Returns the offset of '\r', or npos.
size_t find_crlf_from(const char* data, size_t len, size_t pos) {
    while (pos < len) {
        const void* cr = std::memchr(data + pos, '\r', len - pos);
        if (!cr) return std::string_view::npos;
        size_t idx = static_cast<const char*>(cr) - data;
        if (idx + 1 >= len) return std::string_view::npos;  // '\n' not arrived yet
        if (data[idx + 1] == '\n') return idx;
        pos = idx + 1;
    }
    return std::string_view::npos;
}

// A header line that never terminates is either garbage or an attack: refuse
// to buffer it forever.
ParseResult missing_crlf_result(size_t buffered_after_type_byte) {
    return buffered_after_type_byte > RespParser::MAX_INLINE_LENGTH ? ParseResult::ERROR
                                                                     : ParseResult::INCOMPLETE;
}

} // anonymous namespace

ParseResult RespParser::parse(ByteBuffer& buffer, RespValue& out_value) {
    if (buffer.empty()) {
        return ParseResult::INCOMPLETE;
    }
    size_t saved_index = buffer.reader_index();
    ParseResult res = parse_internal(buffer, out_value, 0);
    if (res == ParseResult::INCOMPLETE) {
        buffer.set_reader_index(saved_index);
    }
    return res;
}

ParseResult RespParser::parse_internal(ByteBuffer& buffer, RespValue& out_value, int depth) {
    if (buffer.empty()) {
        return ParseResult::INCOMPLETE;
    }

    char type_byte = *buffer.peek();
    switch (type_byte) {
        case '+': return parse_simple_string(buffer, out_value);
        case '-': return parse_error(buffer, out_value);
        case ':': return parse_integer(buffer, out_value);
        case '$': return parse_bulk_string(buffer, out_value);
        case '*': return parse_array(buffer, out_value, depth);
        default:  return ParseResult::ERROR;
    }
}

ParseResult RespParser::parse_simple_string(ByteBuffer& buffer, RespValue& out_value) {
    const char* crlf = buffer.find_crlf();
    if (!crlf) return missing_crlf_result(buffer.readable_bytes());

    const char* start = buffer.peek() + 1; // skip '+'
    size_t len = crlf - start;
    out_value = RespValue::make_simple_string(std::string(start, len));
    buffer.retrieve((crlf + 2) - buffer.peek());
    return ParseResult::OK;
}

ParseResult RespParser::parse_error(ByteBuffer& buffer, RespValue& out_value) {
    const char* crlf = buffer.find_crlf();
    if (!crlf) return missing_crlf_result(buffer.readable_bytes());

    const char* start = buffer.peek() + 1; // skip '-'
    size_t len = crlf - start;
    out_value = RespValue::make_error(std::string(start, len));
    buffer.retrieve((crlf + 2) - buffer.peek());
    return ParseResult::OK;
}

ParseResult RespParser::parse_integer(ByteBuffer& buffer, RespValue& out_value) {
    const char* crlf = buffer.find_crlf();
    if (!crlf) return missing_crlf_result(buffer.readable_bytes());

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
    if (!crlf) return missing_crlf_result(buffer.readable_bytes());

    const char* start = buffer.peek() + 1; // skip '$'
    std::string_view sv(start, crlf - start);
    int64_t len = 0;
    if (!parse_int64(sv, len)) {
        return ParseResult::ERROR;
    }

    if (len < -1 || len > MAX_BULK_LENGTH) {
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
    if (data_start[len] != '\r' || data_start[len + 1] != '\n') {
        return ParseResult::ERROR; // payload longer than its declared length
    }
    out_value = RespValue::make_bulk_string(std::string(data_start, static_cast<size_t>(len)));
    buffer.retrieve(total_required);
    return ParseResult::OK;
}

ParseResult RespParser::parse_array(ByteBuffer& buffer, RespValue& out_value, int depth) {
    if (depth >= MAX_NESTING_DEPTH) {
        return ParseResult::ERROR;
    }

    size_t saved_index = buffer.reader_index();

    const char* crlf = buffer.find_crlf();
    if (!crlf) return missing_crlf_result(buffer.readable_bytes());

    const char* start = buffer.peek() + 1; // skip '*'
    std::string_view sv(start, crlf - start);
    int64_t count = 0;
    if (!parse_int64(sv, count)) {
        return ParseResult::ERROR;
    }

    if (count < -1 || count > MAX_MULTIBULK_LENGTH) {
        return ParseResult::ERROR;
    }

    if (count == -1) {
        out_value = RespValue::make_null_array();
        buffer.retrieve((crlf + 2) - buffer.peek());
        return ParseResult::OK;
    }

    buffer.retrieve((crlf + 2) - buffer.peek());

    RespArray arr;
    // Never trust the declared count for the up-front allocation: grow as
    // elements actually arrive.
    arr.reserve(static_cast<size_t>(std::min<int64_t>(count, 1024)));

    for (int64_t i = 0; i < count; ++i) {
        RespValue elem;
        ParseResult res = parse_internal(buffer, elem, depth + 1);
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

// Parses "*<n>\r\n" followed by n arguments without recursion and without
// copying anything until the whole frame has arrived. Elements may be bulk
// strings ($), or for convenience simple strings (+) / integers (:).
ParseResult RespParser::parse_multibulk_command(ByteBuffer& buffer, std::vector<std::string>& out_cmd) {
    const char* data = buffer.peek();
    const size_t avail = buffer.readable_bytes();

    size_t eol = find_crlf_from(data, avail, 1);
    if (eol == std::string_view::npos) return missing_crlf_result(avail);

    int64_t count = 0;
    if (!parse_int64(std::string_view(data + 1, eol - 1), count) ||
        count < -1 || count > MAX_MULTIBULK_LENGTH) {
        return ParseResult::ERROR;
    }
    size_t pos = eol + 2;

    // (offset, length) of every argument inside the buffer
    std::vector<std::pair<size_t, size_t>> spans;
    spans.reserve(static_cast<size_t>(std::min<int64_t>(std::max<int64_t>(count, 0), 1024)));

    for (int64_t i = 0; i < count; ++i) {
        if (pos >= avail) return ParseResult::INCOMPLETE;
        const char type = data[pos];
        size_t line_end = find_crlf_from(data, avail, pos + 1);
        if (line_end == std::string_view::npos) return missing_crlf_result(avail - pos);

        if (type == '$') {
            int64_t len = 0;
            if (!parse_int64(std::string_view(data + pos + 1, line_end - pos - 1), len) ||
                len < 0 || len > MAX_BULK_LENGTH) {
                return ParseResult::ERROR;
            }
            size_t payload = line_end + 2;
            size_t needed = payload + static_cast<size_t>(len) + 2;
            if (needed > avail) return ParseResult::INCOMPLETE;
            if (data[payload + len] != '\r' || data[payload + len + 1] != '\n') {
                return ParseResult::ERROR;
            }
            spans.emplace_back(payload, static_cast<size_t>(len));
            pos = needed;
        } else if (type == '+' || type == ':') {
            spans.emplace_back(pos + 1, line_end - pos - 1);
            pos = line_end + 2;
        } else {
            return ParseResult::ERROR;
        }
    }

    out_cmd.clear();
    out_cmd.reserve(spans.size());
    for (const auto& [off, len] : spans) {
        out_cmd.emplace_back(data + off, len);
    }
    buffer.retrieve(pos);
    return ParseResult::OK;
}

ParseResult RespParser::parse_inline_command(ByteBuffer& buffer, std::vector<std::string>& out_cmd) {
    const char* eol = buffer.find_eol();
    if (!eol) {
        return buffer.readable_bytes() > MAX_INLINE_LENGTH ? ParseResult::ERROR : ParseResult::INCOMPLETE;
    }

    size_t line_len = eol - buffer.peek();
    if (line_len > MAX_INLINE_LENGTH) {
        return ParseResult::ERROR;
    }
    if (line_len > 0 && *(eol - 1) == '\r') {
        line_len -= 1;
    }

    std::string_view line(buffer.peek(), line_len);
    std::string error;
    bool ok = tokenize_command_line(line, out_cmd, error);
    buffer.retrieve((eol + 1) - buffer.peek());

    if (!ok) {
        out_cmd.clear();
        return ParseResult::ERROR;
    }
    return ParseResult::OK;
}

ParseResult RespParser::parse_command(ByteBuffer& buffer, std::vector<std::string>& out_cmd) {
    // Blank inline lines and empty arrays ("*0\r\n") are no-ops. Skip them here
    // instead of returning INCOMPLETE: that would stall any pipelined commands
    // already sitting in the buffer behind them.
    while (!buffer.empty()) {
        ParseResult res = (*buffer.peek() == '*') ? parse_multibulk_command(buffer, out_cmd)
                                                  : parse_inline_command(buffer, out_cmd);
        if (res != ParseResult::OK) {
            return res;
        }
        if (!out_cmd.empty()) {
            return ParseResult::OK;
        }
    }
    return ParseResult::INCOMPLETE;
}

} // namespace miniredis
