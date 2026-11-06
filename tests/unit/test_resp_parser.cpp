#include "protocol/resp_parser.hpp"
#include "protocol/resp_serializer.hpp"
#include <cassert>
#include <iostream>

using namespace miniredis;

void test_simple_string() {
    ByteBuffer buf;
    buf.append("+OK\r\n");

    RespParser parser;
    RespValue val;
    ParseResult res = parser.parse(buf, val);

    assert(res == ParseResult::OK);
    assert(val.type == RespType::SIMPLE_STRING);
    assert(val.get_string() == "OK");
    assert(buf.empty());
    std::cout << "[PASS] test_simple_string\n";
}

void test_integer_and_error() {
    ByteBuffer buf;
    buf.append(":1000\r\n-ERR unknown\r\n");

    RespParser parser;
    RespValue v1, v2;

    assert(parser.parse(buf, v1) == ParseResult::OK);
    assert(v1.type == RespType::INTEGER);
    assert(v1.get_integer() == 1000);

    assert(parser.parse(buf, v2) == ParseResult::OK);
    assert(v2.type == RespType::ERROR);
    assert(v2.get_string() == "ERR unknown");
    assert(buf.empty());
    std::cout << "[PASS] test_integer_and_error\n";
}

void test_bulk_string() {
    ByteBuffer buf;
    buf.append("$6\r\nfoobar\r\n$-1\r\n");

    RespParser parser;
    RespValue v1, v2;

    assert(parser.parse(buf, v1) == ParseResult::OK);
    assert(v1.type == RespType::BULK_STRING);
    assert(!v1.is_null);
    assert(v1.get_string() == "foobar");

    assert(parser.parse(buf, v2) == ParseResult::OK);
    assert(v2.type == RespType::BULK_STRING);
    assert(v2.is_null);
    assert(buf.empty());
    std::cout << "[PASS] test_bulk_string\n";
}

void test_array_and_command() {
    ByteBuffer buf;
    buf.append("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");

    RespParser parser;
    std::vector<std::string> cmd;
    ParseResult res = parser.parse_command(buf, cmd);

    assert(res == ParseResult::OK);
    assert(cmd.size() == 3);
    assert(cmd[0] == "SET");
    assert(cmd[1] == "mykey");
    assert(cmd[2] == "myvalue");
    assert(buf.empty());
    std::cout << "[PASS] test_array_and_command\n";
}

void test_inline_command() {
    ByteBuffer buf;
    buf.append("SET mykey \"hello world\"\r\n");

    RespParser parser;
    std::vector<std::string> cmd;
    ParseResult res = parser.parse_command(buf, cmd);

    assert(res == ParseResult::OK);
    assert(cmd.size() == 3);
    assert(cmd[0] == "SET");
    assert(cmd[1] == "mykey");
    assert(cmd[2] == "hello world");
    assert(buf.empty());
    std::cout << "[PASS] test_inline_command\n";
}

void test_inline_single_quotes() {
    ByteBuffer buf;
    buf.append("SET a 'hello haha'\r\n");

    RespParser parser;
    std::vector<std::string> cmd;
    ParseResult res = parser.parse_command(buf, cmd);

    assert(res == ParseResult::OK);
    assert(cmd.size() == 3);
    assert(cmd[2] == "hello haha");
    assert(buf.empty());
    std::cout << "[PASS] test_inline_single_quotes\n";
}

void test_inline_unbalanced_quotes() {
    ByteBuffer buf;
    buf.append("SET a 'hello\r\n");

    RespParser parser;
    std::vector<std::string> cmd;
    ParseResult res = parser.parse_command(buf, cmd);

    assert(res == ParseResult::ERROR);
    assert(cmd.empty());
    assert(buf.empty());
    std::cout << "[PASS] test_inline_unbalanced_quotes\n";
}

void test_inline_closing_quote_followed_by_text() {
    ByteBuffer buf;
    buf.append("SET a \"hi\"there\r\n");

    RespParser parser;
    std::vector<std::string> cmd;
    ParseResult res = parser.parse_command(buf, cmd);

    assert(res == ParseResult::ERROR);
    std::cout << "[PASS] test_inline_closing_quote_followed_by_text\n";
}

int main() {
    std::cout << "Running RESP Parser Unit Tests...\n";
    test_simple_string();
    test_integer_and_error();
    test_bulk_string();
    test_array_and_command();
    test_inline_command();
    test_inline_single_quotes();
    test_inline_unbalanced_quotes();
    test_inline_closing_quote_followed_by_text();
    std::cout << "All RESP Parser Unit Tests Passed!\n";
    return 0;
}
