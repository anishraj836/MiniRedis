#pragma once

#include "protocol/resp_types.hpp"
#include "common/byte_buffer.hpp"
#include <vector>
#include <string>
#include <string_view>
#include <optional>

namespace miniredis {

enum class ParseResult {
    OK,
    INCOMPLETE,
    ERROR
};

class RespParser {
public:
    RespParser() = default;

    /**
     * @brief Parse next complete RESP value from input buffer.
     * Consumes parsed bytes from buffer on success.
     */
    ParseResult parse(ByteBuffer& buffer, RespValue& out_value);

    /**
     * @brief Parses a complete Redis command (vector of string tokens) from buffer.
     * Handles both RESP Array format (*3\r\n$3\r\nSET...) and Plain Inline format (SET k v\r\n).
     */
    ParseResult parse_command(ByteBuffer& buffer, std::vector<std::string>& out_cmd);

private:
    ParseResult parse_internal(ByteBuffer& buffer, RespValue& out_value);
    ParseResult parse_simple_string(ByteBuffer& buffer, RespValue& out_value);
    ParseResult parse_error(ByteBuffer& buffer, RespValue& out_value);
    ParseResult parse_integer(ByteBuffer& buffer, RespValue& out_value);
    ParseResult parse_bulk_string(ByteBuffer& buffer, RespValue& out_value);
    ParseResult parse_array(ByteBuffer& buffer, RespValue& out_value);
    ParseResult parse_inline_command(ByteBuffer& buffer, std::vector<std::string>& out_cmd);
};

} // namespace miniredis
