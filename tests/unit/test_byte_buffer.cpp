#include "common/byte_buffer.hpp"
#include <cassert>
#include <iostream>
#include <string_view>

using namespace miniredis;

void test_byte_buffer_basic() {
    ByteBuffer buf(128);
    assert(buf.readable_bytes() == 0);
    assert(buf.empty());

    buf.append("HELLO WORLD", 11);
    assert(buf.readable_bytes() == 11);
    assert(!buf.empty());
    assert(buf.string_view() == "HELLO WORLD");

    std::string s = buf.retrieve_as_string(5);
    assert(s == "HELLO");
    assert(buf.readable_bytes() == 6);
    assert(buf.string_view() == " WORLD");

    buf.append(" AGAIN\r\n", 8);
    const char* crlf = buf.find_crlf();
    assert(crlf != nullptr);
    assert(std::string_view(crlf, 2) == "\r\n");

    buf.retrieve_all();
    assert(buf.empty());
    std::cout << "[PASS] test_byte_buffer_basic\n";
}

void test_byte_buffer_growth() {
    ByteBuffer buf(16);
    std::string large(10000, 'A');
    buf.append(large);
    assert(buf.readable_bytes() == 10000);
    assert(buf.retrieve_all_as_string() == large);
    assert(buf.empty());
    std::cout << "[PASS] test_byte_buffer_growth\n";
}

int main() {
    std::cout << "Running ByteBuffer Unit Tests...\n";
    test_byte_buffer_basic();
    test_byte_buffer_growth();
    std::cout << "All ByteBuffer Unit Tests Passed!\n";
    return 0;
}
