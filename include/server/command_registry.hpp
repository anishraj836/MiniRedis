#pragma once

#include "protocol/command.hpp"
#include "protocol/resp_types.hpp"
#include "storage/storage_engine.hpp"
#include "common/atomic_stats.hpp"
#include <string>
#include <unordered_map>
#include <functional>
#include <memory>

namespace miniredis {

class RedisServer;

using CommandHandler = std::function<RespValue(const Command&, StorageEngine&, RedisServer*)>;

// Which arguments are keys, using Redis' convention: token indexes
// first_key..last_key stepping by key_step (last_key -1 = the final token).
// first_key == 0: no keys. ALL_KEYS: the command touches the whole keyspace.
struct KeySpec {
    static constexpr int ALL_KEYS = -1;
    int first_key{0};
    int last_key{0};
    int key_step{1};
};

struct CommandMeta {
    std::string name;
    int arity; // Positive: exact arity. Negative: minimum arity (e.g. -2 means >= 2 args)
    CommandFlag flags{CommandFlag::NONE};
    CommandHandler handler;
    KeySpec keys{};
};

class CommandRegistry {
public:
    CommandRegistry();
    ~CommandRegistry() = default;

    void register_command(std::string name, int arity, CommandFlag flags, CommandHandler handler,
                          KeySpec keys = {});

    RespValue dispatch(const Command& cmd, StorageEngine& engine, RedisServer* server);

    [[nodiscard]] const CommandMeta* find_command(const std::string& upper_name) const;

    // The key arguments of `cmd` according to meta.keys (bounds-checked, so a
    // command with the wrong number of arguments simply yields fewer keys).
    [[nodiscard]] static std::vector<std::string_view> keys_of(const CommandMeta& meta, const Command& cmd);

    [[nodiscard]] const std::unordered_map<std::string, CommandMeta>& all_commands() const noexcept {
        return commands_;
    }

private:
    void register_all_commands();

    std::unordered_map<std::string, CommandMeta> commands_;
};

} // namespace miniredis
