# MiniRedis: Architecture Notes

MiniRedis is a learning project: an in-memory key-value server written in C++17 that speaks the Redis protocol (RESP2). It runs a multi-threaded event loop (`kqueue` on macOS, `epoll` on Linux), splits the keyspace across lock-protected shards, evicts with per-shard LRU, expires keys lazily and in the background, and persists writes to an append-only file (AOF) with online compaction (`BGREWRITEAOF`).

It is **not** a production database and not a drop-in Redis replacement. It implements a subset of commands, has no replication, clustering, RDB snapshots, transactions, pub/sub or scripting, and has only been tested on one laptop. These notes explain how each part works, why it was built that way, and where it falls short.

---

## Contents

1. [Request flow](#1-request-flow)
2. [Networking](#2-networking)
3. [Protocol parsing](#3-protocol-parsing)
4. [Storage and concurrency](#4-storage-and-concurrency)
5. [LRU eviction](#5-lru-eviction)
6. [Key expiration](#6-key-expiration)
7. [Persistence (AOF)](#7-persistence-aof)
8. [Memory accounting](#8-memory-accounting)
9. [Benchmarks and how to read them](#9-benchmarks-and-how-to-read-them)
10. [Comparison with Redis](#10-comparison-with-redis)
11. [Known limitations](#11-known-limitations)
12. [Bugs found in a review pass](#12-bugs-found-in-a-review-pass)

---

## 1. Request flow

```
 clients
    |  TCP
    v
+---------------------------+
| main event loop           |  accepts connections (polled by a 1 ms timer),
|                           |  runs the TTL sweeper every 100 ms
+-------------+-------------+
              | round-robin hand-off of the new socket
              v
+---------------------------+   +---------------------------+
| I/O loop thread 0         |   | I/O loop thread N-1       |   (default N = 4)
| kqueue / epoll            |   | kqueue / epoll            |
| readv -> ByteBuffer       |   | readv -> ByteBuffer       |
| RESP parser               |   | RESP parser               |
+-------------+-------------+   +-------------+-------------+
              |                               |
              v                               v
+-------------------------------------------------------------+
| RedisServer::execute()                                      |
|   read command  -> dispatch                                 |
|   write command -> rewrite relative TTLs to absolute ones,  |
|                    lock the keys' write stripes,            |
|                    dispatch, append to AOF, unlock          |
+-----------------------------+-------------------------------+
                              |  murmur3(key) % shard_count
                              v
+-------------------------------------------------------------+
| StorageEngine: 64 shards by default                         |
|   each shard: std::shared_mutex, unordered_map, LRU list,   |
|               TTL min-heap + map                            |
+-----------------------------+-------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| AofLogger: in-memory buffer, background flusher thread,     |
| rewrite delta buffer used during BGREWRITEAOF               |
+-------------------------------------------------------------+
```

The path of one command:

1. **Accept.** The main loop calls `accept()` on the non-blocking listening socket and hands the new connection to an I/O loop chosen round-robin. Acceptance is driven by a 1 ms timer on the main loop rather than by a readiness event; this is simple but adds up to about a millisecond of connect latency.
2. **Read.** The I/O loop gets a readable event and calls `readv()` into the connection's `ByteBuffer` (see [2.3](#23-bytebuffer-and-readv)).
3. **Parse.** `RespParser::parse_command()` extracts one command at a time. If the frame is incomplete, it consumes nothing and waits for more bytes.
4. **Execute.** `RedisServer::execute()` looks the command up in `CommandRegistry`, checks the argument count and runs the handler. Write commands do extra work, described in [4.4](#44-write-commands-striped-key-locks) and [7.1](#71-what-gets-logged).
5. **Reply.** Replies for every command parsed from one read are batched into a single string and sent together. `Connection::send()` tries to write directly; leftover bytes go to an output buffer and write interest is enabled until it drains.

---

## 2. Networking

Code: [src/net/](src/net/), [include/net/](include/net/)

### 2.1 Threading model

The server uses a main loop plus a fixed pool of I/O loops (a "multi-reactor" design, as in muduo or Netty):

- The **main loop** owns the listening socket, the periodic TTL sweeper and the stats timer.
- Each **I/O loop** runs on its own thread with its own poller. A connection stays on one I/O loop for its whole life, so its reads, parsing, command execution and writes all happen on that thread.

Commands from different connections can therefore run truly in parallel, which is the main difference from Redis's single-threaded command execution. It is also the source of most of the concurrency problems this project has had to solve (see [4](#4-storage-and-concurrency) and [12](#12-bugs-found-in-a-review-pass)).

Cross-thread work is posted with `EventLoop::queue_in_loop()`, which appends to a mutex-protected task list and wakes the target loop by writing 8 bytes to a non-blocking pipe (the "self-pipe trick").

### 2.2 Pollers and write interest

A small `Poller` interface has three implementations: `KqueuePoller` (macOS/BSD), `EpollPoller` (Linux, level-triggered, with `EPOLLRDHUP`) and `PollPoller` (fallback). The kqueue event array starts at 256 entries and doubles when a poll fills it.

All pollers are used in level-triggered mode, so write interest must only be enabled while there is something to send. Otherwise a writable socket would wake the loop on every iteration and spin the CPU. `Connection` handles this as follows:

1. `send_in_loop()` first tries a direct `write()`.
2. Anything the kernel did not accept is copied into `output_buffer_`, and write interest is enabled.
3. `handle_write()` flushes the buffer and disables write interest once it is empty.

There is currently no limit on `output_buffer_`, so a client that never reads its replies can make the server buffer without bound (see [11](#11-known-limitations)).

### 2.3 ByteBuffer and readv

`ByteBuffer` is a contiguous `std::vector<char>` with read and write indexes and 8 bytes of prepend space, the same layout as muduo's buffer:

```
+-------------------+------------------+------------------+
| prependable bytes |  readable bytes  |  writable bytes  |
+-------------------+------------------+------------------+
0      <=      read_index_    <=   write_index_   <=   size()
```

When space runs out it first slides the unread bytes to the front (`memmove`), and only grows if that is not enough.

`read_fd()` calls `readv()` with two buffers: the free space inside the `ByteBuffer`, and a 64 KB array on the stack. One system call can therefore read more than the buffer currently has room for; any overflow is appended afterwards, which may grow the buffer once. This keeps the default buffer small (16 KB) without forcing extra `read()` calls for larger requests. I have not measured how much this saves compared with a simpler approach.

### 2.4 Connection lifetime and shutdown

Connections are `std::shared_ptr`s owned by `TcpServer`. When a connection closes, `TcpServer`'s close hook removes it from its map; the last reference is dropped on the connection's I/O thread, and the socket is closed by RAII. Server code that wants to know about closes registers with `TcpServer::set_close_callback()` rather than on the connection itself (overwriting the connection's hook leaked one file descriptor per client; see [12](#12-bugs-found-in-a-review-pass)).

After `QUIT` or a protocol error, a connection is half-closed: the remaining replies are flushed, and any further input is discarded instead of executed.

`RedisServer::stop()` only sets a flag and wakes the main loop, so it is safe to call from a signal handler. The main loop thread performs the actual teardown after its loop exits: stop accepting, close connections, stop the I/O threads, wait for a running rewrite, and flush and close the AOF.

---

## 3. Protocol parsing

Code: [src/protocol/resp_parser.cpp](src/protocol/resp_parser.cpp)

### 3.1 RESP2 in brief

| First byte | Type | Example |
|---|---|---|
| `+` | Simple string | `+OK\r\n` |
| `-` | Error | `-ERR unknown command\r\n` |
| `:` | Integer | `:1000\r\n` |
| `$` | Bulk string | `$5\r\nhello\r\n` (`$-1\r\n` is null) |
| `*` | Array | `*2\r\n$3\r\nGET\r\n$1\r\nk\r\n` |

Clients send commands as arrays of bulk strings.

### 3.2 Handling partial frames

TCP delivers a byte stream, so a command can arrive split at any byte. The command parser walks the frame using offsets into the buffer and only copies arguments into `std::string`s once the whole frame is present. If anything is missing it returns `INCOMPLETE` without consuming any bytes, and parsing restarts from the beginning of the frame when more data arrives. A regression test checks that every possible cut point of a frame is handled this way.

The parser is not zero-copy: every argument is copied once into the `Command`'s token vector. Restarting from the start of a frame also means a very large, slowly arriving command is re-scanned on each read; this is cheap because nothing is copied until the frame is complete.

### 3.3 Limits

The parser rejects input that would otherwise make the server allocate or buffer without bound. These limits match Redis's defaults:

| Limit | Value |
|---|---|
| Elements per command | 1,048,576 |
| Bytes per argument | 512 MB |
| Inline command / header line | 64 KB |
| Nesting depth when parsing replies | 32 |

Exceeding a limit is a protocol error: the server replies `-ERR Protocol error` and closes the connection, because the stream is no longer frame-aligned. Before these limits existed, a single 22-byte header (`*9223372036854775807\r\n`) crashed the server.

### 3.4 Inline commands

If a request does not start with `*`, it is parsed as an inline command (for example `PING\r\n` typed into `telnet` or `nc`). The tokenizer follows `redis-cli`'s rules: whitespace separates arguments, single or double quotes group text, `\"`, `\'` and `\\` are escapes, and an unbalanced quote is an error.

---

## 4. Storage and concurrency

Code: [src/storage/](src/storage/), [include/storage/](include/storage/)

### 4.1 Shards

The keyspace is split into `shard_count` shards (64 by default), chosen with 32-bit MurmurHash3:

```
shard = murmur3_32(key) % shard_count
```

Each `StorageShard` holds:

- `std::shared_mutex mutex_`
- `std::unordered_map<std::string, ValueObject> data_`
- `LRUCache lru_`
- `TTLManager ttl_manager_`
- its memory counter

Reads take the shard's lock in shared mode, so several threads can read one shard at once; writes take it exclusively. Two writes only wait for each other when their keys land in the same shard. I have not measured contention under realistic workloads, so I can't say how much this helps in practice compared with, for example, fewer shards or a single lock.

### 4.2 Values

```cpp
struct ValueObject {
    RedisType type;          // STRING, HASH, LIST, SET
    int64_t expire_at_ms;    // -1 = no expiry
    std::variant<std::monostate,
                 std::string,
                 std::unordered_map<std::string, std::string>,
                 std::deque<std::string>,
                 std::unordered_set<std::string>> data;
};
```

`std::variant` keeps the value inline in the map node and avoids a virtual class hierarchy. On this machine (libc++, arm64) `sizeof(ValueObject)` is 72 bytes; the containers themselves still allocate on the heap.

Every operation checks the type first. Using a command on a key of the wrong type returns `WRONGTYPE`, as in Redis, and does not modify the key.

### 4.3 Multi-key commands

`MSET` and `MGET` touch several shards. To avoid deadlock, they collect the shard indexes they need, sort them, and lock in ascending order. Because every multi-shard operation uses the same order, two of them can never wait on each other in a cycle.

- `MGET` holds all of its shard locks in shared mode while reading, so it sees a consistent snapshot. A concurrent `MSET` is visible either entirely or not at all.
- `MSET` reserves memory for all values before writing any of them, so if it would exceed `maxmemory`, nothing is written.

### 4.4 Write commands: striped key locks

The shard lock protects one storage call. Many commands make more than one call: `INCR` reads, parses and writes; `SETNX` checks then sets; `HINCRBY` reads a field then writes it. Without something extra, two clients can interleave between those calls and lose an update.

`RedisServer::execute()` therefore holds a second set of locks around every write command. `KeyLockTable` ([include/common/key_lock_table.hpp](include/common/key_lock_table.hpp)) is an array of 1024 mutexes; a key maps to one of them by hash. Before a write command runs, the server works out which arguments are keys (each command declares this, as in Redis's command table), locks their stripes in ascending order, runs the command, appends it to the AOF and unlocks. `FLUSHALL` and `FLUSHDB` lock every stripe.

This gives three properties:

1. **Atomic read-modify-write.** No other write to the same key can run between the read and the write.
2. **AOF order matches apply order.** Two writes to the same key are logged in the order they were applied, so replay reaches the same state.
3. **A quiescent point for rewrites.** Taking every stripe waits for in-flight writes to finish (see [7.4](#74-bgrewriteaof)).

Reads do not take stripe locks. Lock order is always stripes first, then shard locks, which keeps the two levels from deadlocking.

The cost is one more mutex acquisition per write. In my measurements throughput was unchanged within run-to-run noise, but those were single runs on one machine (see [9](#9-benchmarks-and-how-to-read-them)).

---

## 5. LRU eviction

Code: [src/storage/lru_cache.cpp](src/storage/lru_cache.cpp), [src/storage/storage_shard.cpp](src/storage/storage_shard.cpp)

### 5.1 Data structure

Each shard keeps a classic LRU: a `std::list<std::string>` of keys (front = most recently used) and an `unordered_map` from key to list iterator. Touching a key is `list.splice()` to the front and eviction pops the back, both O(1). The cost is two extra copies of every key plus list and map node overhead per entry, which is more metadata than Redis's approximate LRU uses.

LRU order is **per shard**, not global. Eviction removes the least recently used key of the shard that needs space, which may not be the least recently used key in the whole database.

### 5.2 Updating LRU order during reads

A `GET` holds the shard lock in shared mode, but moving its key to the front of the list is a write to the list. Taking the shard lock exclusively for every read would serialize reads, and `std::shared_mutex` cannot be upgraded in place. So the list has its own small lock: a spinlock (`SpinLock`, [include/common/spinlock.hpp](include/common/spinlock.hpp)) held only for the `splice()`. It spins with a CPU pause hint (`yield` on ARM, `_mm_pause` on x86) and exponential backoff, then yields the thread.

A spinlock suits this because the critical section is a few pointer updates. I have not benchmarked it against a plain `std::mutex` here, so I can't quantify the difference; on an uncontended path a `std::mutex` is also cheap.

### 5.3 Policies

When a write would push memory over `maxmemory`, the shard evicts its own keys until the write fits or it runs out of candidates:

| Policy | Victim |
|---|---|
| `noeviction` | none; the write fails with `OOM` |
| `allkeys-lru` | least recently used key in the shard |
| `volatile-lru` | least recently used key that has a TTL (scans up to 64 keys from the LRU end, then falls back to the soonest-expiring key) |
| `volatile-ttl` | key closest to expiring |
| `allkeys-random` | key from a random non-empty hash bucket |

All evictions go through one erase function that removes the key from the map, the LRU list and the TTL index together, and each one is counted in `INFO`'s `evicted_keys`. The policy and limit can be changed at runtime with `CONFIG SET maxmemory` and `CONFIG SET maxmemory-policy`.

Because eviction is per shard, a write can fail with `OOM` when its shard has nothing left to evict, even though other shards still hold evictable keys.

---

## 6. Key expiration

Code: [src/storage/ttl_manager.cpp](src/storage/ttl_manager.cpp), [src/storage/storage_engine.cpp](src/storage/storage_engine.cpp)

Expiry times are absolute Unix milliseconds (`expire_at_ms`) based on the wall clock, so moving the system clock changes when keys expire.

### 6.1 Lazy expiration

Every access checks whether the key has expired. If it has, the read lock is released, the exclusive lock is taken, the key is checked again (another thread may have removed it in between) and deleted, and the command behaves as if the key were missing.

### 6.2 Active expiration

Lazy expiry alone never frees keys that are never read again, so the main loop runs `active_expire_cycle()` every 100 ms. Each shard's `TTLManager` keeps:

- `expiry_map_`: key to deadline, for O(1) `TTL` / `PERSIST` lookups;
- `min_heap_`: (deadline, key) pairs, earliest first.

The sweeper pops keys whose deadline has passed from each shard's heap in batches of 20. It moves to the next shard when a shard has nothing more that is due, and stops for this cycle after a 2 ms budget. The next cycle continues where it stopped, so a large burst of expirations is spread across cycles instead of stalling the main loop.

Changing or removing a TTL does not search the heap. The old heap entry is left in place and recognized as stale when it reaches the top (its deadline no longer matches `expiry_map_`). To keep repeated `EXPIRE` calls from growing the heap forever, it is rebuilt from the map when it holds more than twice as many entries as there are live TTLs, plus a slack of 1024.

---

## 7. Persistence (AOF)

Code: [src/aof/](src/aof/), [include/aof/](include/aof/), [src/server/command_rewrite.cpp](src/server/command_rewrite.cpp)

### 7.1 What gets logged

Write commands are logged in RESP form, the same bytes a client would send. Three rules keep replay faithful:

1. **Only successful commands are logged.** A command that returned an error changed nothing. This matters for writes rejected with `OOM`, which would otherwise be applied on restart even though they never happened live.
2. **Relative expiries become absolute.** Before running, write commands are rewritten so replay at a later time has the same effect: `EXPIRE k 10` becomes `PEXPIREAT k <unix-ms>`, `SETEX k 10 v` and `SET k v EX 10` become `SET k v PXAT <unix-ms>`. The rewritten command is what executes and what is logged, so the two always agree. Without this, a key that expired an hour before a restart would come back with a fresh TTL.
3. **Log order matches apply order** for each key, because a command is appended while its stripe locks are still held ([4.4](#44-write-commands-striped-key-locks)).

### 7.2 The logger

`AofLogger` collects serialized commands in an in-memory buffer protected by a mutex. When and how they reach the disk depends on the fsync policy:

| Policy | Behaviour | Can lose on crash |
|---|---|---|
| `always` | the calling thread writes and fsyncs before replying | nothing acknowledged |
| `everysec` (default) | a background thread writes the buffer and fsyncs once a second | about 1 second of writes |
| `no` | the background thread writes once a second, the OS decides when to flush | whatever the OS had not flushed |

All writes to the file go through one mutex, so chunks always reach the file in the order they were appended. Short writes are retried until everything is written. Under `always` the fsync happens while the command still holds its key locks, which makes that policy noticeably slower.

On macOS, `fsync()` returns once data reaches the drive, but the drive may still hold it in a volatile cache. The logger uses `fcntl(fd, F_FULLFSYNC)` there, which asks the drive to flush that cache, and falls back to `fsync()` if the filesystem does not support it. On Linux it uses `fdatasync()`.

### 7.3 Crash recovery

On startup the AOF is replayed into an empty engine before the port is opened:

- A missing file means a fresh start.
- If the file ends in the middle of a command (a crash during a write), the incomplete tail is truncated away and a warning is logged. Leaving it would make later appends unreadable on the next restart.
- Anything else that does not parse, including bytes that are not RESP arrays, is treated as corruption, and the server refuses to start rather than serve a partial dataset. There is no repair tool yet; the file has to be fixed or moved by hand.

### 7.4 BGREWRITEAOF

The AOF only grows; a million `INCR`s on one key are a million lines. A rewrite replaces it with the shortest equivalent: one `SET`, `HSET`, `RPUSH` or `SADD` per key, plus `PEXPIREAT` for keys with a TTL.

Redis takes a consistent snapshot by `fork()`ing; the child sees a copy-on-write image of memory while the parent keeps serving. MiniRedis does not fork. Instead:

1. **Snapshot.** The rewriter takes every key stripe lock, which waits for in-flight writes to finish and blocks new ones. With writes paused it starts the delta buffer and serializes the whole dataset into memory, then releases the locks. Reads keep running during this step.
2. **Write.** The snapshot is written to `<aof>.temp` with no locks held. Writes resume, and each new command is appended both to the live AOF and to the delta buffer.
3. **Drain.** Most of the delta is copied into the temp file while writers keep running.
4. **Swap.** Briefly blocking appends, the rest of the delta is written, the temp file is fsynced and renamed over the AOF, the directory is fsynced so the rename is durable, and the logger switches to the new file.

Because the snapshot is taken while no write is between "applied" and "logged", every command ends up in exactly one of the snapshot or the delta. A regression test checks this by running `INCR` and `RPUSH` from four clients while rewrites run repeatedly, then replaying the AOF and comparing with the live values.

The trade-off: writes pause for as long as it takes to serialize the dataset, and the serialized copy temporarily needs as much memory as the data itself. For a small dataset this is milliseconds; for a large one it could be noticeable. Only one rewrite can run at a time; `SAVE` runs the same procedure synchronously.

---

## 8. Memory accounting

Code: [include/storage/value_object.hpp](include/storage/value_object.hpp), [src/storage/storage_shard.cpp](src/storage/storage_shard.cpp)

MiniRedis does not hook the allocator. It estimates each entry's size from its structure:

```
entry = sizeof(ValueObject) + 48                // map node estimate
      + key.capacity() + sizeof(void*)
      + payload                                 // string capacity, or per-element costs
                                                // plus bucket arrays for hashes and sets
```

`MemoryTracker` keeps the running total in an atomic counter used for `maxmemory` and `INFO`.

The total is maintained incrementally. Adding a list element adds that element's cost and removing it subtracts the same amount, so a write costs O(1) instead of re-estimating the whole collection (which made growing a large list or hash quadratic). Each entry is always charged using the key stored in the map, so adding and removing it use the same numbers. `verify_memory_accounting()` recomputes everything from scratch, and the regression tests use it to check that the counter matches exactly after mixed operations.

"Exact" here means the counter agrees with its own formula. The formula is an estimate: it ignores allocator overhead, fragmentation and `std::deque`'s block layout. I have not compared it with the process's real memory use, so treat `used_memory` as a rough figure.

---

## 9. Benchmarks and how to read them

`miniredis-bench` ([src/client/main_bench.cpp](src/client/main_bench.cpp)) is a simple load generator. These numbers were measured on an Apple M2 laptop (8 cores, 8 GB RAM), with the client and server on the same machine, using one run each:

```
./bin/miniredis-server -p 6401 --aof no
./bin/miniredis-bench  -p 6401 -c 50 -n 200000 -t 4 -P <1|16> -k <SET|GET|INCR>
```

| Mode | SET | GET | INCR |
|---|---|---|---|
| No pipelining, AOF off | ~116k req/s | ~120k req/s | ~113k req/s |
| No pipelining, AOF `everysec` | ~103k req/s | ~113k req/s | ~117k req/s |
| Pipeline depth 16, AOF off | ~378k req/s | ~378k req/s | ~405k req/s |
| Pipeline depth 16, AOF `everysec` | ~397k req/s | ~390k req/s | ~402k req/s |

Without pipelining, SET reported p50 0.03 ms, p99 0.07 ms and max 0.83 ms.

How much weight to give these:

- **Load is lighter than the flags suggest.** Each bench thread sends one request (or one pipeline) at a time and cycles through its connections, so `-t 4 -c 50` means at most 4 requests (or 4 pipelines) in flight, not 50.
- **Pipelined latencies are averages.** In pipelined mode the bench divides each batch's round-trip time by the batch size, so its percentiles are per-command amortized cost, not the latency a client actually waits.
- **Loopback on one machine.** There is no real network, and client and server compete for the same cores.
- **Single runs.** Differences of 10% or so between rows are within run-to-run noise.
- **No comparison with Redis.** I have not run real Redis on the same machine with the same tool, so these numbers say nothing about how MiniRedis compares with it.

Pipelining is still a real effect: sending 16 commands per round trip amortizes system calls and wake-ups, which is why those rows are about 3x higher.

---

## 10. Comparison with Redis

| Area | Redis | MiniRedis | Notes |
|---|---|---|---|
| Command execution | One thread (I/O threads optional since 6.0) | Several I/O threads execute commands in parallel | Parallel execution needs shard locks and key stripes; Redis gets atomicity for free from a single thread. |
| Snapshots for rewrite | `fork()` with copy-on-write | Pause writes, serialize in memory, write in background | Portable and simple, but writes pause during serialization and memory temporarily doubles. Redis avoids the pause but can pay for copy-on-write under heavy writes. |
| Memory accounting | Allocator-level (jemalloc) | Structural estimate | Redis's numbers reflect real allocations; ours are a model. |
| LRU | Approximate, sampled, global | Exact order, per shard | Ours costs more memory per key and is not global. |
| Multi-key commands | Atomic by construction | Sorted shard locks plus key stripes | More machinery to get guarantees Redis gets naturally. |
| Features | Replication, cluster, RDB, transactions, pub/sub, Lua, streams, many data types | Strings, hashes, lists, sets, TTLs, AOF | MiniRedis covers a small subset. |

Redis's single-threaded design is a deliberate choice that keeps it simple and predictable, not a limitation that MiniRedis "fixes". This project explores the other side of that trade-off to learn what it costs.

---

## 11. Known limitations

- **Per-shard eviction.** A write can fail with `OOM` while other shards still have evictable keys.
- **Rewrite pauses writes** while the dataset is serialized, and needs memory for the serialized copy.
- **No output buffer limit.** A client that sends requests but never reads replies makes the server buffer without bound.
- **Accept polling.** New connections are accepted by a 1 ms timer instead of a readiness event.
- **`always` fsyncs under the key locks**, so writes to the same keys wait for the disk.
- **Wall-clock TTLs.** Moving the system clock moves expiry times.
- **Memory numbers are estimates** ([8](#8-memory-accounting)).
- **No AOF repair tool**; mid-file corruption stops startup.
- **Tested on macOS only.** The Linux `epoll` poller exists but was not built or tested as part of this work.
- **Subset of Redis**: no replication, RDB, transactions, pub/sub, scripting, `SCAN`, sorted sets, or `SELECT`.

---

## 12. Bugs found in a review pass

A review of the code turned up several serious bugs. Each has a test in [tests/edge/test_regressions.cpp](tests/edge/test_regressions.cpp) that reproduces it. The main ones:

| Bug | Effect | Fix |
|---|---|---|
| Unbounded array header | One 22-byte packet crashed the server | Parser limits ([3.3](#33-limits)) |
| Close callback overwritten | One leaked file descriptor per client; the server stopped accepting after about 230 clients | Server-level close callback ([2.4](#24-connection-lifetime-and-shutdown)) |
| `INCR`/`SETNX` were read-then-write in separate locks | Lost updates under concurrency (2339 instead of 2400 in a test); `SETNX` could grant a lock twice | Key stripe locks ([4.4](#44-write-commands-striped-key-locks)) |
| Rewrite raced with writers | Writes could be lost or applied twice after `BGREWRITEAOF` | Snapshot with writes paused, atomic swap ([7.4](#74-bgrewriteaof)) |
| Relative TTLs logged as-is | Expired keys came back after a restart | Absolute deadlines in the log ([7.1](#71-what-gets-logged)) |
| Torn AOF tail kept | Commands appended after a crash were unreadable on the next restart | Truncate the tail ([7.3](#73-crash-recovery)) |
| `INCR`/`APPEND` on a hash | Silently overwrote the hash | `WRONGTYPE` checks |
| Collection memory re-estimated per write | Growing one list and one hash to 50k elements took 12.5 s; now 37 ms | Incremental accounting ([8](#8-memory-accounting)) |
| Sampler looked at the same 20 keys | Expired keys elsewhere were never reclaimed | Heap-driven sweeper ([6.2](#62-active-expiration)) |
| Shutdown from the signal handler | Use-after-free race found by ThreadSanitizer | Stop only signals; the loop thread tears down ([2.4](#24-connection-lifetime-and-shutdown)) |

All 15 test suites pass under AddressSanitizer + UndefinedBehaviorSanitizer (`make asan`) and ThreadSanitizer (`make tsan`). That shows these tests are clean, not that the code is free of bugs: sanitizers only see the paths the tests exercise.
