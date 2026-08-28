#include "protocol/resp_parser.hpp"
#include "protocol/resp_serializer.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

using namespace miniredis;

// Edge Case 1: 1-Byte Incremental TCP Fragmentation (Feeding byte-by-byte into parser)
void test_byte_by_byte_fragmentation() {
    std::string full_packet = "*3\r\n$3\r\nSET\r\n$7\r\nedgekey\r\n$9\r\nedgevalue\r\n";
    ByteBuffer buffer;
    RespParser parser;
    std::vector<std::string> cmd;

    for (size_t i = 0; i < full_packet.size() - 1; ++i) {
        buffer.append(&full_packet[i], 1);
        cmd.clear();
        ParseResult res = parser.parse_command(buffer, cmd);
        assert(res == ParseResult::INCOMPLETE);
        assert(cmd.empty());
    }

    // Append last byte
    buffer.append(&full_packet.back(), 1);
    ParseResult res = parser.parse_command(buffer, cmd);
    assert(res == ParseResult::OK);
    assert(cmd.size() == 3);
    assert(cmd[0] == "SET");
    assert(cmd[1] == "edgekey");
    assert(cmd[2] == "edgevalue");
    assert(buffer.empty());
    std::cout << "[PASS] test_byte_by_byte_fragmentation\n";
}

// Edge Case 2: Pipelining multiple commands in a single buffer
void test_pipelined_packet_parsing() {
    std::string pipeline_data = 
        "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n"
        "*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n"
        "*1\r\n$4\r\nPING\r\n";
    
    ByteBuffer buffer;
    buffer.append(pipeline_data);
    RespParser parser;

    std::vector<std::string> cmd1, cmd2, cmd3;
    assert(parser.parse_command(buffer, cmd1) == ParseResult::OK);
    assert(cmd1.size() == 3 && cmd1[0] == "SET");

    assert(parser.parse_command(buffer, cmd2) == ParseResult::OK);
    assert(cmd2.size() == 2 && cmd2[0] == "GET");

    assert(parser.parse_command(buffer, cmd3) == ParseResult::OK);
    assert(cmd3.size() == 1 && cmd3[0] == "PING");

    assert(buffer.empty());
    std::cout << "[PASS] test_pipelined_packet_parsing\n";
}

// Edge Case 3: Empty string, Null strings, and Large bulk payloads (1MB)
void test_bulk_string_extremes() {
    ByteBuffer buffer;
    RespParser parser;

    // 1. Empty Bulk String ($0\r\n\r\n)
    buffer.append("$0\r\n\r\n");
    RespValue val_empty;
    assert(parser.parse(buffer, val_empty) == ParseResult::OK);
    assert(val_empty.type == RespType::BULK_STRING);
    assert(!val_empty.is_null);
    assert(val_empty.get_string().empty());

    // 2. Null Bulk String ($-1\r\n)
    buffer.append("$-1\r\n");
    RespValue val_null;
    assert(parser.parse(buffer, val_null) == ParseResult::OK);
    assert(val_null.type == RespType::BULK_STRING);
    assert(val_null.is_null);

    // 3. 1MB Bulk Payload
    std::string large_payload(1024 * 1024, 'Z');
    std::string large_resp = "$" + std::to_string(large_payload.size()) + "\r\n" + large_payload + "\r\n";
    buffer.append(large_resp);
    RespValue val_large;
    assert(parser.parse(buffer, val_large) == ParseResult::OK);
    assert(val_large.get_string().size() == 1024 * 1024);
    assert(val_large.get_string() == large_payload);
    assert(buffer.empty());

    std::cout << "[PASS] test_bulk_string_extremes\n";
}

// Edge Case 4: Malformed and Corrupted Protocol Frames
void test_malformed_frames() {
    ByteBuffer buffer;
    RespParser parser;
    RespValue val;

    // Unknown header type byte '?'
    buffer.append("?INVALID\r\n");
    assert(parser.parse(buffer, val) == ParseResult::ERROR);
    buffer.retrieve_all();

    // Negative array size other than -1
    buffer.append("*-5\r\n");
    assert(parser.parse(buffer, val) == ParseResult::ERROR);
    buffer.retrieve_all();

    // Invalid length integer
    buffer.append("$abc\r\nhello\r\n");
    assert(parser.parse(buffer, val) == ParseResult::ERROR);
    buffer.retrieve_all();

    std::cout << "[PASS] test_malformed_frames\n";
}

// Edge Case 5: Complex inline command parsing with quotes and extra spaces
void test_inline_command_spaces_and_quotes() {
    ByteBuffer buffer;
    RespParser parser;
    std::vector<std::string> cmd;

    // Excess leading/trailing spaces and quotes
    buffer.append("   SET    \"my complex key\"    \"hello world \\\"escaped\\\"\"    \r\n");
    assert(parser.parse_command(buffer, cmd) == ParseResult::OK);
    assert(cmd.size() == 3);
    assert(cmd[0] == "SET");
    assert(cmd[1] == "my complex key");
    assert(cmd[2] == "hello world \"escaped\"");
    assert(buffer.empty());

    std::cout << "[PASS] test_inline_command_spaces_and_quotes\n";
}

int main() {
    std::cout << "Running Protocol Edge-Case Tests...\n";
    test_byte_by_byte_fragmentation();
    test_pipelined_packet_parsing();
    test_bulk_string_extremes();
    test_malformed_frames();
    test_inline_command_spaces_and_quotes();
    std::cout << "All Protocol Edge-Case Tests Passed!\n";
    return 0;
}
