#include "client/redis_client.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>

void print_resp(const miniredis::RespValue& val, int indent = 0, bool raw = false) {
    if (val.is_null) {
        if (raw) std::cout << "\n";
        else std::cout << "(nil)\n";
        return;
    }

    std::string ind(indent * 2, ' ');

    switch (val.type) {
        case miniredis::RespType::SIMPLE_STRING:
            if (raw) std::cout << val.get_string() << "\n";
            else std::cout << ind << "\033[32m" << val.get_string() << "\033[0m\n";
            break;
        case miniredis::RespType::ERROR:
            if (raw) std::cout << val.get_string() << "\n";
            else std::cout << ind << "\033[31m(error) " << val.get_string() << "\033[0m\n";
            break;
        case miniredis::RespType::INTEGER:
            if (raw) std::cout << val.get_integer() << "\n";
            else std::cout << ind << "\033[33m(integer) " << val.get_integer() << "\033[0m\n";
            break;
        case miniredis::RespType::BULK_STRING:
            if (raw) std::cout << val.get_string() << "\n";
            else std::cout << ind << "\"" << val.get_string() << "\"\n";
            break;
        case miniredis::RespType::ARRAY: {
            const auto& arr = val.get_array();
            if (arr.empty()) {
                if (!raw) std::cout << ind << "(empty array)\n";
                return;
            }
            for (size_t i = 0; i < arr.size(); ++i) {
                if (!raw) std::cout << ind << (i + 1) << ") ";
                print_resp(arr[i], indent + 1, raw);
            }
            break;
        }
        case miniredis::RespType::NULL_VALUE:
            if (!raw) std::cout << ind << "(nil)\n";
            break;
    }
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 6379;
    bool raw_mode = false;
    std::vector<std::string> direct_args;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (arg == "--raw") {
            raw_mode = true;
        } else {
            // Collect remainder as direct command
            for (; i < argc; ++i) {
                direct_args.push_back(argv[i]);
            }
            break;
        }
    }

    miniredis::RedisClient client;
    if (!client.connect(host, port)) {
        std::cerr << "Could not connect to MiniRedis at " << host << ":" << port << ": Connection refused\n";
        return 1;
    }

    // Direct command execution mode
    if (!direct_args.empty()) {
        auto resp = client.execute(direct_args);
        print_resp(resp, 0, raw_mode);
        return 0;
    }

    // Interactive REPL mode
    std::cout << "Connected to MiniRedis at " << host << ":" << port << "\n";
    std::cout << "Type 'help' for available commands, or 'exit' / 'quit' to close.\n\n";

    std::string line;
    while (true) {
        std::cout << host << ":" << port << "> " << std::flush;
        if (!std::getline(std::cin, line)) {
            break;
        }

        if (line.empty()) continue;

        // Check for quit/exit
        std::string trim_line = line;
        while (!trim_line.empty() && (trim_line.front() == ' ' || trim_line.front() == '\t')) trim_line.erase(0, 1);
        if (trim_line == "exit" || trim_line == "quit" || trim_line == "QUIT") {
            break;
        }

        // Tokenize line respecting quotes
        std::vector<std::string> tokens;
        std::string token;
        bool in_quotes = false;
        for (char c : line) {
            if (c == '"') {
                in_quotes = !in_quotes;
            } else if ((c == ' ' || c == '\t') && !in_quotes) {
                if (!token.empty()) {
                    tokens.push_back(token);
                    token.clear();
                }
            } else {
                token += c;
            }
        }
        if (!token.empty()) tokens.push_back(token);

        if (tokens.empty()) continue;

        auto resp = client.execute(tokens);
        print_resp(resp, 0, raw_mode);
    }

    std::cout << "Bye!\n";
    return 0;
}
