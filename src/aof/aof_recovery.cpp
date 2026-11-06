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
    char chunk[65536];
    size_t bytes_read_total = 0;
    size_t bytes_consumed = 0; // end offset of the last complete command

    LOG_INFO("Replaying AOF persistence file: " + filename);

    while (true) {
        ssize_t n = ::read(fd, chunk, sizeof(chunk));
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR("Failed to read AOF file " + filename + ": " + strerror(errno));
            ::close(fd);
            return false;
        }
        if (n > 0) {
            buffer.append(chunk, static_cast<size_t>(n));
            bytes_read_total += static_cast<size_t>(n);
        }

        while (true) {
            std::vector<std::string> cmd_tokens;
            size_t before = buffer.readable_bytes();
            // The logger only ever writes RESP arrays. Anything else would be
            // accepted by the inline-command parser and silently skipped as an
            // unknown command, hiding corruption.
            ParseResult res = (!buffer.empty() && *buffer.peek() != '*')
                                  ? ParseResult::ERROR
                                  : parser.parse_command(buffer, cmd_tokens);
            bytes_consumed += before - buffer.readable_bytes();

            if (res == ParseResult::OK) {
                Command cmd(std::move(cmd_tokens));
                registry.dispatch(cmd, engine, nullptr);
                ++replayed_count;
            } else if (res == ParseResult::INCOMPLETE) {
                break; // Need more data from file
            } else {
                LOG_ERROR("AOF file " + filename + " is corrupted at byte offset " + std::to_string(bytes_consumed) +
                          " (after command #" + std::to_string(replayed_count) + "). Refusing to load a partial "
                          "dataset; repair or move the file away and restart.");
                ::close(fd);
                return false;
            }
        }

        if (n == 0) {
            break;
        }
    }
    ::close(fd);

    if (bytes_consumed < bytes_read_total) {
        // Torn write at the end of the file (crash or power loss mid-append).
        // Cut it off: otherwise new commands would be appended after the
        // garbage and become unreadable on the next restart.
        LOG_WARN("AOF file " + filename + " ends with an incomplete command (" +
                 std::to_string(bytes_read_total - bytes_consumed) + " bytes). Truncating to offset " +
                 std::to_string(bytes_consumed) + ".");
        if (::truncate(filename.c_str(), static_cast<off_t>(bytes_consumed)) < 0) {
            LOG_ERROR("Failed to truncate AOF file " + filename + ": " + strerror(errno));
            return false;
        }
    }

    LOG_INFO("AOF Recovery complete. Successfully reconstituted " + std::to_string(replayed_count) + " commands.");
    return true;
}

} // namespace miniredis
