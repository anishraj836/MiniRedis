#pragma once

#include "common/types.hpp"
#include <atomic>
#include <cstddef>
#include <string>

namespace miniredis {

class MemoryTracker {
public:
    explicit MemoryTracker(size_t max_memory = DEFAULT_MAX_MEMORY,
                           EvictionPolicy policy = EvictionPolicy::ALLKEYS_LRU)
        : max_memory_(max_memory),
          policy_(policy) {}

    void add_memory(size_t bytes) noexcept {
        used_memory_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void sub_memory(size_t bytes) noexcept {
        size_t current = used_memory_.load(std::memory_order_relaxed);
        while (current > 0) {
            size_t next = (current >= bytes) ? (current - bytes) : 0;
            if (used_memory_.compare_exchange_weak(current, next, std::memory_order_relaxed)) {
                break;
            }
        }
    }

    [[nodiscard]] size_t get_used_memory() const noexcept {
        return used_memory_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] size_t get_max_memory() const noexcept {
        return max_memory_.load(std::memory_order_relaxed);
    }

    void set_max_memory(size_t bytes) noexcept {
        max_memory_.store(bytes, std::memory_order_relaxed);
    }

    [[nodiscard]] EvictionPolicy get_eviction_policy() const noexcept {
        return policy_.load(std::memory_order_relaxed);
    }

    void set_eviction_policy(EvictionPolicy policy) noexcept {
        policy_.store(policy, std::memory_order_relaxed);
    }

    [[nodiscard]] bool is_out_of_memory(size_t needed_bytes = 0) const noexcept {
        size_t max_mem = max_memory_.load(std::memory_order_relaxed);
        if (max_mem == 0) return false; // 0 = unlimited memory
        return (used_memory_.load(std::memory_order_relaxed) + needed_bytes) > max_mem;
    }

    [[nodiscard]] std::string format_memory_human() const;

private:
    std::atomic<size_t> used_memory_{0};
    std::atomic<size_t> max_memory_{DEFAULT_MAX_MEMORY};
    std::atomic<EvictionPolicy> policy_{EvictionPolicy::ALLKEYS_LRU};
};

} // namespace miniredis
