# MiniRedis: Interview Prep Notes

Notes for explaining and defending this project in an interview. The rule for everything here: **only claim what the code does and what has been measured.** When something was not measured, say so. Interviewers usually trust "I didn't measure that, but here's how I would" more than a confident number they can poke holes in.

For the full design write-up see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Contents

1. [The pitch](#1-the-pitch)
2. [Request lifecycle](#2-request-lifecycle)
3. [Subsystem cheat sheet](#3-subsystem-cheat-sheet)
4. [Design choices and alternatives](#4-design-choices-and-alternatives)
5. [Trade-offs](#5-trade-offs)
6. [Failure modes and how they are handled](#6-failure-modes-and-how-they-are-handled)
7. [Interview questions](#7-interview-questions)
8. [Numbers worth remembering](#8-numbers-worth-remembering)
9. [Glossary](#9-glossary)
10. [Symbol reference](#10-symbol-reference)

---

## 1. The pitch

### 30 seconds

> "MiniRedis is an in-memory key-value server I wrote in C++17 that speaks the Redis protocol, so `redis-cli`-style clients can talk to it. Unlike Redis, it executes commands on several threads, so most of the interesting work was concurrency: sharded storage with reader-writer locks, per-key write locks so commands like `INCR` stay atomic, and an append-only file with online compaction that has to stay correct while clients keep writing. It's a learning project, not a Redis replacement: it supports strings, hashes, lists, sets and TTLs, and I've only tested it on my laptop."

### If they want more

- **Networking:** one accept loop hands connections round-robin to a fixed pool of event-loop threads (kqueue on macOS, epoll on Linux).
- **Storage:** 64 shards by default, each with a `std::shared_mutex`, a hash map, a per-shard LRU list and a TTL min-heap.
- **Correctness under concurrency:** write commands hold striped per-key locks while they execute and while they're logged, which makes read-modify-write commands atomic and keeps the log in apply order.
- **Persistence:** AOF with `always` / `everysec` / `no` fsync policies, crash recovery that repairs a torn last write, and `BGREWRITEAOF` without `fork()`.
- **Testing:** 15 test suites, including a regression suite with a test for each serious bug I found, all clean under ASan/UBSan and TSan.

### What not to say

- "Production-grade", "faster than Redis", "scales linearly with cores", "lock-free", "zero-copy" or "guarantees zero data loss". None of these are true or measured.
- Benchmark numbers without their caveats ([section 8](#8-numbers-worth-remembering)).

---

## 2. Request lifecycle

What happens for `SET user:1 alice`:

1. **Accept.** The main loop's 1 ms timer calls `accept()` on the listening socket and gives the connection to the next I/O loop in round-robin order. That loop owns the connection from then on.
2. **Read.** The I/O loop's poller reports the socket readable. `ByteBuffer::read_fd()` calls `readv()` into the buffer's free space plus a 64 KB stack array, so one system call can read more than the buffer currently holds.
3. **Parse.** `RespParser::parse_command()` checks that the whole frame has arrived. If not, it consumes nothing and returns `INCOMPLETE`. If so, it copies the arguments into a `std::vector<std::string>`.
4. **Normalize.** For write commands, relative expiries are rewritten to absolute times (`SET k v EX 10` becomes `SET k v PXAT <unix-ms>`), so the logged command replays to the same state.
5. **Lock keys.** The server looks up which arguments are keys and locks their stripes in `KeyLockTable` in ascending order.
6. **Execute.** The handler hashes the key (`murmur3_32(key) % 64`), takes that shard's lock exclusively, updates the map, the LRU list, the TTL index and the memory counter, evicting first if `maxmemory` would be exceeded.
7. **Log.** If the command succeeded and AOF is on, its RESP bytes are appended to the logger's buffer, still under the stripe locks.
8. **Reply.** The reply is serialized into a batch with the replies of any other commands from the same read and written to the socket. Bytes the kernel can't take yet go to the output buffer, with write interest enabled until it drains.

A `GET` skips steps 4, 5 and 7, takes the shard lock in shared mode, and briefly takes the shard's LRU spinlock to move the key to the front of the list.

---

## 3. Subsystem cheat sheet

### Networking ([src/net/](src/net/))

- Main loop: listening socket (accepted via a 1 ms timer), TTL sweeper every 100 ms, stats every 500 ms.
- I/O loops: default 4 threads, each with its own poller; cross-thread work is posted to a task queue and the loop is woken through a pipe.
- Level-triggered polling; write interest is only enabled while there is unsent data, to avoid spinning.
- `stop()` only signals the main loop, so it is safe in a signal handler; the loop thread does the teardown.

### Protocol ([src/protocol/resp_parser.cpp](src/protocol/resp_parser.cpp))

- RESP2 arrays of bulk strings, plus inline commands (`PING\r\n`) with `redis-cli` quoting rules.
- Limits: 1M elements per command, 512 MB per argument, 64 KB per line, nesting depth 32 when parsing replies. Exceeding one is a protocol error that closes the connection.
- Not zero-copy: arguments are copied once, after the frame is complete.

### Storage ([src/storage/](src/storage/))

- `ValueObject` = type + `expire_at_ms` + `std::variant` of string / hash / list / set; 72 bytes with libc++ on arm64.
- Reads use shared locks, writes exclusive. `MSET`/`MGET` lock shards in ascending order; `MGET` reads a consistent snapshot; `MSET` is all-or-nothing on OOM.
- Wrong-type operations return `WRONGTYPE` and change nothing.

### Concurrency for writes ([include/common/key_lock_table.hpp](include/common/key_lock_table.hpp))

- 1024 mutex stripes; a write command locks the stripes of all its keys (sorted) for the duration of execute + log. `FLUSHALL` locks all of them.
- Lock order is stripes, then shards, so the two levels can't deadlock.

### Eviction

- Per-shard exact LRU (list + map, O(1) touch and evict). Not global.
- Policies: `noeviction`, `allkeys-lru`, `volatile-lru`, `volatile-ttl`, `allkeys-random`. Changeable with `CONFIG SET`.

### Expiration

- Lazy check on every access, plus an active sweeper that pops due keys from each shard's TTL min-heap (batches of 20, 2 ms budget per cycle, resumes at the next shard).
- Stale heap entries are skipped when popped; the heap is rebuilt if stale entries dominate.

### AOF ([src/aof/](src/aof/))

- Buffer in memory; background thread writes once a second (`everysec` also fsyncs); `always` writes and fsyncs before replying.
- One mutex serializes file writes; short writes are retried; macOS uses `F_FULLFSYNC`, Linux `fdatasync`.
- Only successful commands are logged, with relative TTLs made absolute.
- Recovery truncates a torn last command; any other corruption stops startup.
- Rewrite: pause writes, serialize to memory, resume, write the temp file, drain the delta, then swap atomically.

---

## 4. Design choices and alternatives

| Area | Choice | Alternatives considered | Why, and what I'd reconsider |
|---|---|---|---|
| Threading | Multi-reactor: accept loop + fixed I/O loop pool | Single-threaded loop like Redis; thread per connection | I wanted to learn what parallel execution costs. A single thread would have avoided almost every bug in [section 6](#6-failure-modes-and-how-they-are-handled). Thread per connection doesn't scale to many idle clients. |
| Event loop | Own `Poller` over kqueue / epoll / poll | libuv, Boost.Asio | Mostly for learning. A library would have been less code and fewer bugs; I don't claim it would be slower. |
| Storage locking | Fixed shards with `std::shared_mutex` | One global lock; a concurrent hash map | Simple, and reads can run in parallel. I haven't measured contention, so I can't say 64 is the right number. |
| Write atomicity | Striped key locks around execute + log | Atomic primitives inside each storage call; one global write lock | Stripes fix atomicity, AOF ordering and the rewrite snapshot with one mechanism. Shard-level primitives would also work for atomicity but not for log ordering. |
| LRU | Exact per-shard list + map | Redis-style sampled approximate LRU | Exact LRU is easy to reason about, but costs extra memory per key and is not global. Sampling would use less memory. |
| LRU on reads | Separate spinlock for the list | Exclusive shard lock on every read | Keeps reads parallel. Not benchmarked against a plain `std::mutex`, which might be just as good. |
| Values | `std::variant` | Class hierarchy with virtual functions | Value types are a closed set, so a variant is natural, type-safe and avoids one heap allocation per value. |
| Expiration | Lazy + heap-driven sweeper | Redis-style random sampling | The heap finds every due key in order. The cost is extra memory for heap entries. |
| Rewrite snapshot | Pause writes and serialize in memory | `fork()` with copy-on-write; per-shard snapshots | Portable and correct, but writes pause during serialization. Making that pause shorter would be the next improvement. |
| Hash | MurmurHash3 (32-bit) | `std::hash`, cryptographic hashes | Same result across standard libraries and fast; cryptographic strength isn't needed. |
| Protocol | RESP2 | Custom binary protocol | Lets existing Redis tooling talk to it. |

---

## 5. Trade-offs

**Parallel execution vs. simplicity.** Running commands on several threads is the whole point of the project, and also its main source of bugs. Redis gets atomic commands, ordered logging and consistent snapshots for free by running commands on one thread. Here each of those needed explicit locking, and each was broken at some point (see [section 6](#6-failure-modes-and-how-they-are-handled)).

**Exact LRU vs. memory.** A list node and a map entry per key, each holding a copy of the key, is more metadata than Redis's 24-bit LRU clock per object. In exchange the order is exact within a shard.

**Estimated memory vs. allocator truth.** No allocator hooks, so it is portable, but `used_memory` is a model. It is self-consistent (a test checks the counter against a full recount), but I haven't compared it with the process's real memory use.

**No `fork()` vs. write pauses.** The rewrite works anywhere, but writes pause while the dataset is serialized and memory temporarily doubles. `fork()` avoids the pause at the cost of copy-on-write overhead under heavy writes.

**Per-shard eviction vs. global eviction.** Evicting only within the shard that needs space avoids locking other shards, but a write can fail with OOM while other shards still have evictable keys.

---

## 6. Failure modes and how they are handled

Each of these has a test, mostly in [tests/edge/](tests/edge/). Several were real bugs found during a review and are now covered by [tests/edge/test_regressions.cpp](tests/edge/test_regressions.cpp).

| Scenario | What happens now | What used to happen |
|---|---|---|
| Command split across many TCP packets, down to 1 byte each | Parser waits without consuming bytes, then parses the whole frame | Worked |
| Huge array or bulk header (`*9223372036854775807`) | Protocol error, connection closed, server keeps running | Server crashed (uncaught `std::length_error`) |
| Deeply nested arrays | Rejected | Unbounded recursion |
| Many clients connecting and disconnecting | File descriptors are released | One fd leaked per client; accept failed after ~230 clients |
| Client resets the connection mid-write | `SIGPIPE` ignored, error handled, connection cleaned up | Worked |
| Two clients `INCR` the same key | Both increments counted | Updates lost |
| Two clients `SETNX` the same key | Exactly one succeeds | Both could succeed |
| `QUIT` pipelined after other commands | Earlier replies sent, then the connection closes; later input ignored | Earlier replies dropped; later commands executed silently |
| `INCR` / `APPEND` on a hash | `WRONGTYPE`, hash untouched | Hash overwritten with a string |
| `INCR` on a key with a TTL | TTL kept | Key became persistent |
| Integer overflow in `INCR`, `EXPIRE`... | Error | Signed overflow (undefined behaviour) |
| Memory hits `maxmemory` | Eviction per policy, or `OOM` under `noeviction`, for all data types | Only `SET` was checked; collections grew freely |
| Overwriting a big value with a small one | Memory counter shrinks accordingly | Worked for strings; collections drifted |
| Keys with TTLs that are never read again | Reclaimed by the heap-driven sweeper | Sampler could miss them forever |
| Crash in the middle of an AOF write | Tail truncated on restart; later appends readable | Tail kept; everything appended after the restart was lost on the next restart |
| Corruption in the middle of the AOF | Server refuses to start | Loaded a partial dataset |
| Restart after keys expired | They stay expired | Came back with a fresh TTL |
| `BGREWRITEAOF` while clients write | Replay matches the live dataset | Writes could be lost or applied twice |
| Ctrl+C | Clean shutdown on the loop thread | Signal handler did unsafe work; TSan found a use-after-free on shutdown |

Still not handled ([ARCHITECTURE.md section 11](ARCHITECTURE.md#11-known-limitations)): a client that never reads replies can grow the output buffer without limit.

---

## 7. Interview questions

### Q1. Redis is single-threaded on purpose. Why make this multi-threaded?

> "To learn what it costs. Redis's single thread is a good design: commands are atomic by construction and the log and snapshots are easy to keep consistent. I wanted to see what you have to build to keep those guarantees when commands run in parallel. The answer turned out to be quite a lot: shard locks, per-key write locks, ordered logging, and a careful rewrite protocol. I found real bugs in each of those along the way. I haven't benchmarked it against Redis, so I wouldn't claim it's faster; on a laptop with loopback networking I'd expect Redis to be competitive or better."

### Q2. How is `INCR` atomic if it reads and then writes?

> "Each storage call is protected by its shard's lock, but `INCR` makes two calls: a read and a write. Originally nothing held a lock across both, and with 8 clients doing 300 increments each the counter ended at 2339 instead of 2400.
>
> The fix is a table of 1024 mutex stripes. Every write command declares which arguments are keys; before it runs, the server locks those keys' stripes in ascending order and holds them until the command has been executed and appended to the AOF. Another write to the same key has to wait, so the read-modify-write can't interleave. Reads don't take stripes, and stripes are always taken before shard locks, so there's a fixed lock order. The same test now gets exactly 2400, and it runs clean under ThreadSanitizer."

### Q3. How do multi-key commands avoid deadlock?

> "Any operation that needs several shard locks sorts the shard indexes and locks them in ascending order. If everybody uses the same order, you can't get a cycle of threads each holding one lock and waiting for another. The same rule applies to the key stripes. `MGET` holds its shard locks while reading, so it sees a consistent snapshot, and `MSET` reserves memory for everything before writing anything, so it's all-or-nothing. An earlier version computed the sorted shard set and then never actually locked anything; I caught that in review."

### Q4. Why a spinlock for the LRU list?

> "A `GET` holds the shard lock in shared mode so reads can run in parallel, but moving the key to the front of the LRU list writes to the list. `std::shared_mutex` can't be upgraded to exclusive in place, and taking it exclusively on every read would serialize reads. So the list has its own small lock, held only for a `splice`, which is a few pointer updates. A spinlock is a reasonable fit for such a short section. I haven't benchmarked it against `std::mutex`, which is also cheap when uncontended, so I wouldn't claim a specific win."

### Q5. How does `BGREWRITEAOF` work without `fork()`?

> "The goal is a file equal to 'snapshot at time T, plus every write after T'. The hard part is getting T exactly right while other threads keep writing. My first version got it wrong in three ways: writes could land in both the snapshot and the delta, so `INCR`s were applied twice on replay; writes flushed between finishing the delta and switching files went to the old file and were lost; and buffered bytes were copied into the new file twice.
>
> Now the rewriter takes every key stripe lock. Since write commands hold their stripes until they're applied *and* logged, this waits for in-flight writes and blocks new ones, so there's a clean cut. At that moment it starts the delta buffer and serializes the dataset into memory, then releases the locks. Reads keep running throughout. The snapshot is written to a temp file without locks, most of the delta is drained while writers continue, and a final short step under the logger's locks writes the rest, fsyncs, renames over the AOF, fsyncs the directory and switches the file descriptor.
>
> The cost is a write pause proportional to the dataset size and a temporary in-memory copy. `fork()` avoids the pause. A test runs `INCR` and `RPUSH` from four clients while rewriting repeatedly, then replays the file and checks it matches the live values exactly."

### Q6. What happens if the server crashes mid-write?

> "The last command in the AOF may be cut off. On startup, replay stops at the incomplete command and truncates the file there, then logs a warning. Before I added the truncation, new commands were appended after the garbage, so on the *next* restart replay stopped at the garbage and everything after it was lost. Anything that isn't a clean tail, like corruption in the middle, stops startup instead of silently loading part of the data. How much is lost depends on the fsync policy: nothing that was acknowledged under `always`, up to about a second under `everysec`."

### Q7. Why `F_FULLFSYNC` on macOS?

> "On macOS, `fsync()` hands the data to the drive, but the drive can keep it in a volatile cache for a while. `fcntl(fd, F_FULLFSYNC)` asks the drive to flush that cache too, so it's what you need for durability across power loss. It's much slower. On Linux I use `fdatasync`, which flushes the data and the metadata needed to read it (like file size) but skips things like timestamps. If `F_FULLFSYNC` isn't supported by the filesystem, it falls back to `fsync`."

### Q8. How do TTLs work, and how do you avoid scanning the heap when a TTL changes?

> "Two mechanisms. Every access checks the key's deadline and deletes it if it has passed. Separately, every 100 ms a sweeper pops keys whose deadline has passed from each shard's min-heap, in batches, with a 2 ms budget so a big burst doesn't stall the loop.
>
> Changing or removing a TTL only updates a side map; the old heap entry stays and is recognized as stale when it reaches the top, because its deadline no longer matches the map. That keeps updates O(log n). To stop repeated `EXPIRE` calls from growing the heap forever, it's rebuilt when stale entries dominate. The first version used a 'sampler' that always looked at the same 20 entries, so expired keys elsewhere could stay in memory indefinitely; switching to the heap fixed that."

### Q9. Expired keys came back after a restart. Why, and how did you fix it?

> "The AOF logged commands exactly as received. `EXPIRE k 10` replayed a day later gives the key a fresh 10 seconds. Now, before a write command runs, relative expiries are rewritten to absolute deadlines: `EXPIRE k 10` becomes `PEXPIREAT k <unix-ms>`, and `SET k v EX 10` becomes `SET k v PXAT <unix-ms>`. The rewritten command is what executes and what's logged, so they can't disagree. Redis does the same kind of translation when it propagates commands."

### Q10. How do you know any of this is correct?

> "Mostly through tests that reproduce specific failures, plus sanitizers. Every bug from my review has a regression test that fails on the old code: concurrent `INCR` and `SETNX`, `BGREWRITEAOF` under load, file-descriptor counts after 400 connections, malicious frames, torn AOF files, and memory accounting checked against a full recount. All 15 suites pass under AddressSanitizer + UBSan and ThreadSanitizer, and I ran the full suite several times to look for flakiness. That gives me reasonable confidence in the paths the tests cover, but it's not proof: sanitizers only see code that runs, and I haven't done fuzzing or long-running stress tests yet."

### Q11. Your benchmark shows ~3x more throughput with pipelining. Why?

> "Without pipelining, each command costs a round trip: a wake-up, a read system call, a write system call, and on the client side waiting for the reply. With 16 commands per batch, those costs are shared across 16 commands, and the server answers the whole batch with a single write.
>
> I'd be careful with the numbers themselves. They come from one laptop, with client and server on the same machine, from single runs. The bench tool only keeps one request or batch in flight per thread, so with 4 threads it's 4 at a time even if you ask for 50 clients. In pipelined mode it reports latency as the batch time divided by the batch size, which is an average cost, not what a client waits."

### Q12. What was the hardest bug?

> "Probably the `BGREWRITEAOF` races, because the code looked right and the simple test passed: the existing test only used `SET`, which is idempotent, so applying it twice changes nothing. It only showed up with non-idempotent commands like `INCR` and `RPUSH` running during a rewrite. The fix needed the per-key write locks, so that 'applied' and 'logged' happen together, plus a two-phase swap in the logger.
>
> A close second was a file-descriptor leak: the server registered its own close callback on each connection, which silently replaced the one that freed the socket. Nothing failed until about 230 clients had connected, at which point `accept()` started failing because of macOS's default fd limit."

### Q13. What would you change next?

> "Bound the per-client output buffer so slow readers can't exhaust memory. Make eviction able to free memory from other shards. Shorten the rewrite's write pause, maybe by snapshotting shard by shard. Accept connections on readiness instead of a 1 ms timer. Add fuzzing for the parser and a crash-injection test for the AOF. And run the same benchmark against real Redis, so I can make an honest comparison."

---

## 8. Numbers worth remembering

All measured on an Apple M2 laptop (8 cores, 8 GB), client and server on the same machine, single runs. Quote them with those caveats.

| What | Number |
|---|---|
| Throughput without pipelining (`-c 50 -t 4`) | ~103k–120k req/s for SET / GET / INCR |
| Throughput with pipeline depth 16 | ~378k–405k req/s |
| SET latency without pipelining | p50 0.03 ms, p99 0.07 ms, max 0.83 ms |
| Growing one list + one hash to 50k elements | 12.5 s before the accounting fix, 37 ms after |
| Concurrent `INCR`, 8 clients x 300 | 2339 before the key-lock fix, 2400 after |
| File descriptors after 100 connections | +100 before the close-callback fix, +0 after |
| Packet that crashed the server | 22 bytes: `*9223372036854775807\r\n` |
| `sizeof(ValueObject)` | 72 bytes (libc++, arm64) |
| Defaults | port 6379, 4 I/O threads, 64 shards, 256 MB `maxmemory`, `allkeys-lru`, AOF on with `everysec` |
| Key lock stripes | 1024 |
| Parser limits | 1M elements, 512 MB per argument, 64 KB per line |
| Active expiry | every 100 ms, batches of 20, 2 ms budget |

Commands to reproduce the benchmark:

```
./bin/miniredis-server -p 6401 --aof no
./bin/miniredis-bench  -p 6401 -c 50 -n 200000 -t 4 -P 1  -k SET
./bin/miniredis-bench  -p 6401 -c 50 -n 200000 -t 4 -P 16 -k SET
```

---

## 9. Glossary

- **Multi-reactor:** one event loop accepts connections and hands them to a fixed pool of event loops, each on its own thread, that do the I/O.
- **Non-blocking I/O:** socket calls return immediately with `EAGAIN` instead of sleeping when they can't make progress.
- **Level-triggered vs. edge-triggered:** level-triggered pollers keep reporting a socket as long as it is readable or writable; edge-triggered ones report only changes. MiniRedis uses level-triggered mode and turns write interest off when there is nothing to send.
- **Scatter-gather I/O (`readv`):** one system call reads into several buffers in turn.
- **Self-pipe trick:** waking a thread blocked in `kevent` / `epoll_wait` by writing to a pipe it is watching.
- **RESP2:** Redis's text-based, length-prefixed protocol (`+`, `-`, `:`, `$`, `*`).
- **Lock striping:** protecting many items with a fixed array of locks chosen by hash, instead of one lock or one lock per item.
- **Lock ordering:** always acquiring multiple locks in the same global order, which prevents deadlock cycles.
- **Spinlock:** a lock that busy-waits instead of sleeping; suitable only for very short critical sections.
- **Lazy deletion (heap):** leaving outdated entries in a heap and skipping them when they reach the top, instead of searching for them.
- **AOF:** append-only file; a log of write commands replayed on startup.
- **fsync / `fdatasync` / `F_FULLFSYNC`:** ways to ask the OS (and drive) to make written data durable, with different strength and cost.
- **Torn write:** a write cut off partway by a crash, leaving an incomplete record at the end of a file.
- **Idempotent:** applying an operation twice has the same effect as once (`SET` is, `INCR` is not). It matters for replaying logs.

---

## 10. Symbol reference

| Symbol | Where | What it is |
|---|---|---|
| `ValueObject::type`, `expire_at_ms`, `data` | [include/storage/value_object.hpp](include/storage/value_object.hpp) | Value type tag, absolute expiry in unix ms (-1 = none), and the `std::variant` payload |
| `ByteBuffer::read_index_`, `write_index_`, `buffer_` | [include/common/byte_buffer.hpp](include/common/byte_buffer.hpp) | Read cursor, write cursor, and the `std::vector<char>` storage (16 KB initial + 8 bytes prepend) |
| `RespParser::MAX_MULTIBULK_LENGTH`, `MAX_BULK_LENGTH`, `MAX_INLINE_LENGTH`, `MAX_NESTING_DEPTH` | [include/protocol/resp_parser.hpp](include/protocol/resp_parser.hpp) | Parser limits |
| `EventLoop::wakeup_pipe_`, `pending_tasks_`, `task_mutex_`, `thread_id_` | [include/net/event_loop.hpp](include/net/event_loop.hpp) | Wake-up pipe, cross-thread task queue and its mutex, owning thread (atomic) |
| `Connection::input_buffer_`, `output_buffer_`, `current_events_`, `state_` | [include/net/connection.hpp](include/net/connection.hpp) | Per-connection buffers, registered poller events, lifecycle state |
| `TcpServer::connections_`, `close_cb_` | [include/net/tcp_server.hpp](include/net/tcp_server.hpp) | Live connections (owning), and the user close callback run before cleanup |
| `StorageEngine::shards_`, `shard_count_`, `next_expire_shard_` | [include/storage/storage_engine.hpp](include/storage/storage_engine.hpp) | `std::vector<std::unique_ptr<StorageShard>>`, its size (default 64), where the sweeper resumes |
| `StorageShard::mutex_`, `lru_lock_`, `data_`, `lru_`, `ttl_manager_`, `shard_memory_` | [include/storage/storage_shard.hpp](include/storage/storage_shard.hpp) | Shard reader-writer lock, LRU spinlock, key map, LRU list, TTL index, accounted bytes |
| `StorageShard::WRONG_TYPE`, `OUT_OF_MEMORY` | [include/storage/storage_shard.hpp](include/storage/storage_shard.hpp) | Negative status codes returned by collection writes |
| `LRUCache::key_list_`, `key_map_` | [include/storage/lru_cache.hpp](include/storage/lru_cache.hpp) | Recency list (front = most recent) and key-to-iterator map |
| `TTLManager::min_heap_`, `expiry_map_` | [include/storage/ttl_manager.hpp](include/storage/ttl_manager.hpp) | Deadline heap (with stale entries) and the authoritative key-to-deadline map |
| `MemoryTracker::used_memory_`, `max_memory_`, `evicted_keys_` | [include/storage/memory_tracker.hpp](include/storage/memory_tracker.hpp) | Atomic totals for accounting, the limit, and the eviction counter |
| `KeyLockTable::mutexes_` | [include/common/key_lock_table.hpp](include/common/key_lock_table.hpp) | 1024 write-lock stripes |
| `CommandMeta::keys` (`KeySpec`) | [include/server/command_registry.hpp](include/server/command_registry.hpp) | Which arguments of a command are keys (first, last, step) |
| `AofLogger::primary_buffer_`, `delta_buffer_`, `collecting_delta_` | [include/aof/aof_logger.hpp](include/aof/aof_logger.hpp) | Pending log bytes, rewrite delta, and whether a rewrite is collecting |
| `AofLogger::file_mutex_`, `buffer_mutex_`, `fd_`, `dirty_` | [include/aof/aof_logger.hpp](include/aof/aof_logger.hpp) | Serializes file writes, guards the buffers (lock order file then buffer), current file, written-since-fsync flag |
| `normalize_for_propagation()` | [include/server/command_rewrite.hpp](include/server/command_rewrite.hpp) | Turns relative expiries into absolute deadlines before execution and logging |
