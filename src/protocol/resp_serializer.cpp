#include "protocol/resp_serializer.hpp"

namespace miniredis {

void RespSerializer::serialize(const RespValue& val, std::string& out) {
    if (val.is_null) {
        if (val.type == RespType::ARRAY) {
            out.append("*-1\r\n");
        } else {
            out.append("$-1\r\n");
        }
        return;
    }

    switch (val.type) {
        case RespType::SIMPLE_STRING: {
            out.push_back('+');
            out.append(val.get_string());
            out.append("\r\n");
            break;
        }
        case RespType::ERROR: {
            out.push_back('-');
            out.append(val.get_string());
            out.append("\r\n");
            break;
        }
        case RespType::INTEGER: {
            out.push_back(':');
            out.append(std::to_string(val.get_integer()));
            out.append("\r\n");
            break;
        }
        case RespType::BULK_STRING: {
            const std::string& str = val.get_string();
            out.push_back('$');
            out.append(std::to_string(str.size()));
            out.append("\r\n");
            out.append(str);
            out.append("\r\n");
            break;
        }
        case RespType::ARRAY: {
            const auto& arr = val.get_array();
            out.push_back('*');
            out.append(std::to_string(arr.size()));
            out.append("\r\n");
            for (const auto& elem : arr) {
                serialize(elem, out);
            }
            break;
        }
        case RespType::NULL_VALUE: {
            out.append("$-1\r\n");
            break;
        }
    }
}

std::string RespSerializer::serialize(const RespValue& val) {
    std::string out;
    serialize(val, out);
    return out;
}

std::string RespSerializer::serialize_simple_string(std::string_view str) {
    std::string out;
    out.reserve(str.size() + 3);
    out.push_back('+');
    out.append(str);
    out.append("\r\n");
    return out;
}

std::string RespSerializer::serialize_ok() {
    return "+OK\r\n";
}

std::string RespSerializer::serialize_pong() {
    return "+PONG\r\n";
}

std::string RespSerializer::serialize_error(std::string_view err) {
    std::string out;
    out.reserve(err.size() + 3);
    out.push_back('-');
    out.append(err);
    out.append("\r\n");
    return out;
}

std::string RespSerializer::serialize_integer(int64_t val) {
    std::string str_val = std::to_string(val);
    std::string out;
    out.reserve(str_val.size() + 3);
    out.push_back(':');
    out.append(str_val);
    out.append("\r\n");
    return out;
}

std::string RespSerializer::serialize_bulk_string(std::string_view str) {
    std::string str_len = std::to_string(str.size());
    std::string out;
    out.reserve(1 + str_len.size() + 2 + str.size() + 2);
    out.push_back('$');
    out.append(str_len);
    out.append("\r\n");
    out.append(str);
    out.append("\r\n");
    return out;
}

std::string RespSerializer::serialize_null_bulk() {
    return "$-1\r\n";
}

std::string RespSerializer::serialize_null_array() {
    return "*-1\r\n";
}

std::string RespSerializer::serialize_string_array(const std::vector<std::string>& array) {
    std::string count_str = std::to_string(array.size());
    std::string out;
    out.push_back('*');
    out.append(count_str);
    out.append("\r\n");

    for (const auto& s : array) {
        std::string len_str = std::to_string(s.size());
        out.push_back('$');
        out.append(len_str);
        out.append("\r\n");
        out.append(s);
        out.append("\r\n");
    }
    return out;
}

std::string RespSerializer::serialize_command(const std::vector<std::string>& args) {
    return serialize_string_array(args);
}

} // namespace miniredis
