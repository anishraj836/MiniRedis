#include "aof/aof_recovery.hpp"
#include "protocol/resp_parser.hpp"
#include "server/command_registry.hpp"
#include "common/logger.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace miniredis {

bool AofRecovery::replay_file(const std::string& filename, StorageEngine& engine, CommandRegistry& registry, size_t& replayed_count) {
    replayed_count = 0;
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            // AOF file does not exist yet (fresh server)
            return true;
        }
        LOG_ERROR("Failed to open AOF file for recovery: " + filename + ": " + strerror(errno));
        return false;
    }

    ByteBuffer buffer(65536);
    RespParser parser;
    char chunk[16384];

    LOG_INFO("Replaying AOF persistence file: " + filename);

    while (true) {
        ssize_t bytes_read = ::read(fd, chunk, sizeof(chunk));
        if (bytes_read > 0) {
            buffer.append(chunk, bytes_read);
        }

        // Parse and execute commands from buffer
        while (true) {
            std::vector<std::string> cmd_tokens;
            ParseResult res = parser.parse_command(buffer, cmd_tokens);

            if (res == ParseResult::OK) {
                Command cmd(std::move(cmd_tokens));
                registry.dispatch(cmd, engine, nullptr);
                ++replayed_count;
            } else if (res == ParseResult::INCOMPLETE) {
                break; // Need more data from file
            } else {
                LOG_WARN("Corrupted frame encountered during AOF recovery at command #" + std::to_string(replayed_count + 1));
                break;
            }
        }

        if (bytes_read <= 0) {
            break;
        }
    }

    ::close(fd);
    LOG_INFO("AOF Recovery complete. Successfully reconstituted " + std::to_string(replayed_count) + " commands.");
    return true;
}

} // namespace miniredis
