#pragma once

#include "common/types.hpp"
#include <algorithm>
#include <mutex>
#include <string_view>
#include <utility>
#include <vector>

namespace miniredis {

/**
 * @brief Striped per-key write locks.
 *
 * Every write command holds the stripes of all keys it touches for the whole
 * time it executes *and* is appended to the AOF. That single rule gives:
 *  - atomic read-modify-write commands (INCR, SETNX, APPEND, HINCRBY, SET NX...)
 *    even though they make several storage calls;
 *  - an AOF whose order matches the order writes were applied, so replay
 *    reproduces the same final state;
 *  - a quiescent point for BGREWRITEAOF: lock_all() waits for in-flight writes
 *    to finish, so a snapshot plus the later delta never miss or double-apply
 *    a command.
 *
 * Reads take no stripe locks. Stripes are always acquired in ascending index
 * order, so two commands can never deadlock on each other.
 */
class KeyLockTable {
public:
    explicit KeyLockTable(size_t stripes = 1024) : mutexes_(stripes == 0 ? 1 : stripes) {}

    KeyLockTable(const KeyLockTable&) = delete;
    KeyLockTable& operator=(const KeyLockTable&) = delete;

    class Guard {
    public:
        Guard() = default;
        Guard(KeyLockTable* table, std::vector<size_t> stripes)
            : table_(table), held_(std::move(stripes)) {
            for (size_t idx : held_) table_->mutexes_[idx].lock();
        }
        Guard(Guard&& other) noexcept : table_(other.table_), held_(std::move(other.held_)) {
            other.table_ = nullptr;
            other.held_.clear();
        }
        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;
        Guard& operator=(Guard&&) = delete;
        ~Guard() { release(); }

        void release() noexcept {
            if (!table_) return;
            for (auto it = held_.rbegin(); it != held_.rend(); ++it) {
                table_->mutexes_[*it].unlock();
            }
            held_.clear();
            table_ = nullptr;
        }

    private:
        KeyLockTable* table_{nullptr};
        std::vector<size_t> held_;
    };

    [[nodiscard]] size_t stripe_of(std::string_view key) const noexcept {
        // Different seed from the storage shard hash so stripes and shards are
        // independent.
        return murmur3_32(key.data(), key.size(), 0x5bd1e995u) % mutexes_.size();
    }

    [[nodiscard]] Guard lock_keys(const std::vector<std::string_view>& keys) {
        std::vector<size_t> stripes;
        stripes.reserve(keys.size());
        for (auto key : keys) stripes.push_back(stripe_of(key));
        std::sort(stripes.begin(), stripes.end());
        stripes.erase(std::unique(stripes.begin(), stripes.end()), stripes.end());
        return Guard(this, std::move(stripes));
    }

    [[nodiscard]] Guard lock_all() {
        std::vector<size_t> stripes(mutexes_.size());
        for (size_t i = 0; i < stripes.size(); ++i) stripes[i] = i;
        return Guard(this, std::move(stripes));
    }

    [[nodiscard]] size_t stripe_count() const noexcept { return mutexes_.size(); }

private:
    std::vector<std::mutex> mutexes_;
};

} // namespace miniredis
