#pragma once

#include "protocol/resp_types.hpp"
#include <string>
#include <string_view>
#include <vector>

namespace miniredis {

class RespSerializer {
public:
    static void serialize(const RespValue& val, std::string& out);
    static std::string serialize(const RespValue& val);

    // Direct helper serializers for maximum speed
    static std::string serialize_simple_string(std::string_view str);
    static std::string serialize_ok();
    static std::string serialize_pong();
    static std::string serialize_error(std::string_view err);
    static std::string serialize_integer(int64_t val);
    static std::string serialize_bulk_string(std::string_view str);
    static std::string serialize_null_bulk();
    static std::string serialize_null_array();
    static std::string serialize_string_array(const std::vector<std::string>& array);
    static std::string serialize_command(const std::vector<std::string>& args);
};

} // namespace miniredis
