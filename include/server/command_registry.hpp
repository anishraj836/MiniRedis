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

struct CommandMeta {
    std::string name;
    int arity; // Positive: exact arity. Negative: minimum arity (e.g. -2 means >= 2 args)
    CommandFlag flags{CommandFlag::NONE};
    CommandHandler handler;
};

class CommandRegistry {
public:
    CommandRegistry();
    ~CommandRegistry() = default;

    void register_command(std::string name, int arity, CommandFlag flags, CommandHandler handler);

    RespValue dispatch(const Command& cmd, StorageEngine& engine, RedisServer* server);

    [[nodiscard]] const CommandMeta* find_command(const std::string& upper_name) const;

    [[nodiscard]] const std::unordered_map<std::string, CommandMeta>& all_commands() const noexcept {
        return commands_;
    }

private:
    void register_all_commands();

    std::unordered_map<std::string, CommandMeta> commands_;
};

} // namespace miniredis
