#pragma once

#include <atomic>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <immintrin.h>
#define CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(__arm__)
#define CPU_PAUSE() asm volatile("yield" ::: "memory")
#else
#define CPU_PAUSE() std::this_thread::yield()
#endif

namespace miniredis {

/**
 * @brief High-efficiency, low-overhead atomic spinlock with exponential backoff.
 * Ideal for extremely short critical sections (e.g. LRU node splicing).
 */
class SpinLock {
public:
    SpinLock() noexcept : flag_(false) {}
    ~SpinLock() noexcept = default;

    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;

    void lock() noexcept {
        int backoff = 1;
        while (flag_.exchange(true, std::memory_order_acquire)) {
            while (flag_.load(std::memory_order_relaxed)) {
                for (int i = 0; i < backoff; ++i) {
                    CPU_PAUSE();
                }
                if (backoff < 64) {
                    backoff <<= 1;
                } else {
                    std::this_thread::yield();
                }
            }
        }
    }

    bool try_lock() noexcept {
        return !flag_.load(std::memory_order_relaxed) &&
               !flag_.exchange(true, std::memory_order_acquire);
    }

    void unlock() noexcept {
        flag_.store(false, std::memory_order_release);
    }

private:
    std::atomic<bool> flag_;
};

class SpinLockGuard {
public:
    explicit SpinLockGuard(SpinLock& lock) noexcept : lock_(lock) {
        lock_.lock();
    }
    ~SpinLockGuard() noexcept {
        lock_.unlock();
    }

    SpinLockGuard(const SpinLockGuard&) = delete;
    SpinLockGuard& operator=(const SpinLockGuard&) = delete;

private:
    SpinLock& lock_;
};

} // namespace miniredis
