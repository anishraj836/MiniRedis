#pragma once

#include "protocol/resp_serializer.hpp"
#include <string>
#include <vector>

namespace miniredis {

class AofEntry {
public:
    static std::string serialize_command(const std::vector<std::string>& tokens) {
        return RespSerializer::serialize_string_array(tokens);
    }
};

} // namespace miniredis
