#pragma once

#include <string>
#include <vector>
#include <variant>
#include <cstdint>
#include <string_view>
#include <optional>

namespace miniredis {

enum class RespType : char {
    SIMPLE_STRING = '+',
    ERROR         = '-',
    INTEGER       = ':',
    BULK_STRING   = '$',
    ARRAY         = '*',
    NULL_VALUE    = '_'
};

struct RespValue;

using RespArray = std::vector<RespValue>;

struct RespValue {
    RespType type{RespType::NULL_VALUE};
    std::variant<
        std::monostate,       // Null
        std::string,          // SimpleString, Error, BulkString
        int64_t,              // Integer
        RespArray             // Array
    > data;

    bool is_null{false};

    // Factory helper functions
    static RespValue make_simple_string(std::string str) {
        RespValue v;
        v.type = RespType::SIMPLE_STRING;
        v.data = std::move(str);
        return v;
    }

    static RespValue make_ok() {
        return make_simple_string("OK");
    }

    static RespValue make_pong() {
        return make_simple_string("PONG");
    }

    static RespValue make_error(std::string err) {
        RespValue v;
        v.type = RespType::ERROR;
        v.data = std::move(err);
        return v;
    }

    static RespValue make_integer(int64_t val) {
        RespValue v;
        v.type = RespType::INTEGER;
        v.data = val;
        return v;
    }

    static RespValue make_bulk_string(std::string str) {
        RespValue v;
        v.type = RespType::BULK_STRING;
        v.data = std::move(str);
        return v;
    }

    static RespValue make_null_bulk() {
        RespValue v;
        v.type = RespType::BULK_STRING;
        v.is_null = true;
        return v;
    }

    static RespValue make_array(RespArray arr) {
        RespValue v;
        v.type = RespType::ARRAY;
        v.data = std::move(arr);
        return v;
    }

    static RespValue make_null_array() {
        RespValue v;
        v.type = RespType::ARRAY;
        v.is_null = true;
        return v;
    }

    [[nodiscard]] bool is_string() const noexcept {
        return type == RespType::SIMPLE_STRING || type == RespType::BULK_STRING || type == RespType::ERROR;
    }

    [[nodiscard]] const std::string& get_string() const {
        return std::get<std::string>(data);
    }

    [[nodiscard]] int64_t get_integer() const {
        return std::get<int64_t>(data);
    }

    [[nodiscard]] const RespArray& get_array() const {
        return std::get<RespArray>(data);
    }
};

} // namespace miniredis
