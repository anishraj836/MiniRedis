# MiniRedis ⚡

[![C++17](https://img.shields.io/badge/Language-C%2B%2B17-blue.svg)](https://en.cppreference.com/w/cpp/17)
[![Platform](https://img.shields.io/badge/Platform-macOS%20%7C%20Linux-lightgrey.svg)](https://github.com/)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](https://github.com/)
[![Test Coverage](https://img.shields.io/badge/Tests-14%2F14%20Passing-success.svg)](https://github.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**MiniRedis** is a high-throughput, multi-threaded in-memory key-value database built from scratch in modern **C++17**. It implements the standard **Redis Serialization Protocol (RESP2)**, features a scalable **multi-reactor network architecture** (`epoll`/`kqueue`), fine-grained **64-shard lock-striped concurrency**, $O(1)$ LRU memory eviction, dual-tier TTL expiration, and **Append-Only File (AOF)** durability with live background compaction (`BGREWRITEAOF`).

---

## 📑 Table of Contents

- [Key Features](#-key-features)
- [System Architecture](#-system-architecture)
  - [1. Multi-Reactor Network Engine](#1-multi-reactor-network-engine)
  - [2. 64-Shard Lock-Striped Storage](#2-64-shard-lock-striped-storage)
  - [3. O(1) LRU Eviction with SpinLock Isolation](#3-o1-lru-eviction-with-spinlock-isolation)
  - [4. Dual-Tier TTL Expiration](#4-dual-tier-ttl-expiration)
  - [5. Append-Only File Durability & Live Compaction](#5-append-only-file-durability--live-compaction)
- [Design Trade-offs: MiniRedis vs. Real Redis](#-design-trade-offs-miniredis-vs-real-redis)
- [Supported Commands](#-supported-commands)
- [Performance & Benchmarks](#-performance--benchmarks)
- [Edge-Case Hardening & Resiliency](#-edge-case-hardening--resiliency)
- [Project Directory Layout](#-project-directory-layout)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Building from Source](#building-from-source)
  - [Running the Test Battery](#running-the-test-battery)
  - [Starting the Server](#starting-the-server)
  - [Using the CLI Client](#using-the-cli-client)
  - [Running the Benchmark Tool](#running-the-benchmark-tool)
  - [Running via Docker](#running-via-docker)
- [License](#-license)

---

## 🌟 Key Features

* **⚡ Native RESP2 Protocol:** Zero-copy streaming parser with transactional rollback support for fragmented TCP chunks, bulk binary payloads, and inline command tokens.
* **🌐 Asynchronous Multi-Reactor:** Master-worker event loop design using OS multiplexers (`kqueue` on macOS/BSD, `epoll` on Linux, fallback `poll`) with level-triggered write quenching and `readv` stack vector buffers.
* **🔒 Lock-Striped Concurrency:** Keys are partitioned across 64 discrete shards via MurmurHash3. Reads use `std::shared_lock<std::shared_mutex>` while writes lock only the target shard, enabling multi-core scalability without global lock contention.
* **🧠 $O(1)$ LRU Memory Management:** In-memory doubly-linked list with hash map iterators. Read promotions are protected by hardware-paused spinlocks (`_mm_pause` / ARM `yield`) to eliminate reader-writer lock escalation bottlenecks.
* **⏱️ Dual-Tier TTL Expiration:** Combines lazy/passive expiration (purging expired keys upon access) with an active periodic randomized sampling sweep ($20$ keys/shard every $100\text{ms}$).
* **💾 AOF Persistence & Live Compaction:** Double-buffered asynchronous disk flushing thread with configurable policies (`always`, `everysec`, `no`), crash-recovery replayer, and online background compaction (`BGREWRITEAOF`) with live mutation delta buffering.
* **🛡️ Hardened & Sanitized:** 14 automated test suites (including 6 stress and edge-case suites) clean under AddressSanitizer (`-fsanitize=address,undefined`) and ThreadSanitizer (`-fsanitize=thread`).

---

## 🏛️ System Architecture

```
                                  +-----------------------+
                                  |   Client Connections  |
                                  +-----------+-----------+
                                              |
                                              v
                              +---------------+---------------+
                              |    Main Acceptor Reactor      |
                              |  (Accepts TCP Connections)    |
                              +---------------+---------------+
                                              |
                                              | Round-Robin Dispatch
                                              v
                         +--------------------+--------------------+
                         |                                         |
                         v                                         v
              +---------------------+                   +---------------------+
              |   Worker Reactor 1  |                   |   Worker Reactor N  |
              |  (kqueue / epoll)   |                   |  (kqueue / epoll)   |
              +----------+----------+                   +----------+----------+
                         |                                         |
                         +--------------------+--------------------+
                                              |
                                              v
                                  +-----------+-----------+
                                  |  RESP Protocol Parser |
                                  | (Streaming/Rollback)  |
                                  +-----------+-----------+
                                              |
                                              v
                                  +-----------+-----------+
                                  |    Command Registry   |
                                  |  (String/Hash/List/Set|
                                  +-----------+-----------+
                                              |
                       +----------------------+----------------------+
                       | MurmurHash3(key) % 64                       |
                       v                                             v
        +------------------------------+              +------------------------------+
        |        Storage Shard 0       |              |       Storage Shard 63       |
        |  - std::shared_mutex         |              |  - std::shared_mutex         |
        |  - std::unordered_map        |  ... ... ... |  - std::unordered_map        |
        |  - LRUCache (SpinLock)       |              |  - LRUCache (SpinLock)       |
        |  - TTLManager (Min-Heap)     |              |  - TTLManager (Min-Heap)     |
        +--------------+---------------+              +--------------+---------------+
                       |                                             |
                       +----------------------+----------------------+
                                              |
                                              v
                                  +-----------+-----------+
                                  |  AOF Persistence Log  |
                                  |  - Double-Buffer Async|
                                  |  - BGREWRITEAOF Delta |
                                  +-----------------------+
```

---

### 1. Multi-Reactor Network Engine

The networking layer implements the **Multi-Reactor Pattern**:
* **Acceptor Thread:** The main thread binds the listening TCP socket and registers it with the primary event loop. New connections are accepted in non-blocking mode and distributed across worker threads in a round-robin schedule.
* **Worker Sub-Reactors:** Each worker thread owns an isolated `EventLoop` and `Poller` (`KqueuePoller` or `EpollPoller`), handling I/O events, command parsing, and response serializing independently.
* **Level-Triggered Write Quenching:** When output buffers contain data, write readiness (`POLLOUT`/`EVFILT_WRITE`) is dynamically registered. As soon as all pending bytes are flushed to the socket, write interest is immediately deregistered to prevent 100% CPU spinning.
* **Zero-Allocation Stack Buffer (`readv`):** Each connection utilizes scatter-gather I/O (`readv`) combining the internal `ByteBuffer` with a 64KB stack buffer, allowing large payload ingestion without frequent dynamic heap reallocations.

---

### 2. 64-Shard Lock-Striped Storage

Rather than using a single global mutex (which bottlenecked multi-core throughput) or a lock-free map (which complicates range and collection operations), the keyspace is partitioned into **64 independent shards**:
* **Key Hashing:** Key names are mapped to shard IDs using MurmurHash3:
  $$\text{Shard Index} = \text{MurmurHash3}(\text{key}) \pmod{64}$$
* **Concurrency Control:** Each shard encapsulates its own `std::shared_mutex`. Read operations (`GET`, `HGET`, `LRANGE`, `SMEMBERS`) acquire shared read locks (`std::shared_lock`), enabling unlimited parallel reads across all CPU cores. Write operations (`SET`, `HSET`, `LPUSH`, `SADD`) acquire exclusive locks (`std::unique_lock`) restricted only to the affected shard.
* **Deadlock-Free Multi-Key Operations:** Multi-key commands (`MSET`, `MGET`) calculate all target shard indices, sort them into a `std::set<size_t>`, and acquire shard locks in strictly ascending order. This guarantees zero circular $A \to B$ vs $B \to A$ deadlocks across concurrent threads.

---

### 3. $O(1)$ LRU Eviction with SpinLock Isolation

Each shard maintains an independent **Least Recently Used (LRU)** cache using a doubly-linked list (`std::list<std::string>`) combined with an iterator lookup map (`std::unordered_map<std::string, list::iterator>`):
* **The Promotion Problem:** In standard read-write locks, a read operation (`GET`) needs to move the accessed key to the head of the LRU list. Upgrading a `shared_lock` to a `unique_lock` causes reader starvation and deadlocks.
* **The Solution (SpinLock Isolation):** Data retrieval occurs under `std::shared_lock`, but LRU node promotion (`std::list::splice`) is guarded by a microsecond-duration `SpinLock` using hardware-pause instructions (`_mm_pause` on x86, `__yield()` on ARM). This keeps read operations concurrent while ensuring $O(1)$ LRU tracking with zero memory allocations.

---

### 4. Dual-Tier TTL Expiration

Keys can be assigned precision millisecond time-to-live timestamps:
1. **Passive / Lazy Expiration:** Whenever any key is read or updated, its expiration timestamp is checked against `now_ms()`. If expired, the key is purged on-demand and `nil` is returned.
2. **Active Periodic Sweeper:** Every $100\text{ms}$, an active sweep samples $20$ random keys per shard. If more than $25\%$ of the sampled keys are expired, the cycle repeats immediately until expired keys fall below the threshold, preventing memory leaks on inactive keys.

---

### 5. Append-Only File Durability & Live Compaction

MiniRedis guarantees persistence via an **Append-Only File (AOF)**:
* **Double-Buffered Async Logger:** Incoming mutations serialize into RESP format and append to a thread-safe `primary_buffer_`. A dedicated background flusher thread periodically swaps `primary_buffer_` with `secondary_buffer_` and writes to disk, issuing `fcntl(fd, F_FULLFSYNC)` (macOS) or `fdatasync()` (Linux).
* **Online Background Compaction (`BGREWRITEAOF`):**
  1. A background thread iterates across all 64 shards under shared read locks to capture a point-in-time state snapshot into a temporary file.
  2. Simultaneously, the main server engages a `delta_buffer_` to capture all mutating writes occurring during the rewrite.
  3. Once the snapshot finishes, the accumulated delta is written to the temporary file.
  4. The temporary file is atomically renamed (`::rename`) over the active AOF file, and the logger reopens its file descriptor to ensure zero lost mutations.

---

## ⚖️ Design Trade-offs: MiniRedis vs. Real Redis

| Dimension | MiniRedis Architecture | Official Redis Architecture | Rationale & Trade-off |
| :--- | :--- | :--- | :--- |
| **Concurrency Model** | Multi-threaded multi-reactor + 64-shard lock striping | Single-threaded event loop (Redis 6.0+ uses I/O threads for networking only) | MiniRedis scales computation and memory lookups natively across multi-core CPUs; Redis avoids all locking overhead by keeping storage strictly single-threaded. |
| **Snapshot Durability** | Multi-threaded snapshot iteration + live mutation delta buffer | Linux kernel `fork()` using hardware Copy-on-Write (CoW) | MiniRedis remains cross-platform portable without relying on OS `fork()` semantics; Redis achieves zero-latency lock-free snapshots via kernel page tables. |
| **Memory Tracking** | Analytical structural estimation ($\sim 95\%$ accuracy) | Allocator hooks into `jemalloc`/`zmalloc` | MiniRedis calculates memory based on `sizeof(T)` + container capacities; official Redis tracks exact physical heap page allocations via custom allocators. |
| **Multi-Key Operations** | Sorted shard lock acquisition (`std::set<size_t>`) | Global single-threaded access | MiniRedis provides deadlock-free multi-key transactions across shards at the cost of acquiring multiple mutexes; Redis executes multi-key commands with zero lock overhead. |

---

## 💻 Supported Commands

MiniRedis implements **35 core Redis commands** with full RESP2 compliance:

### 🔤 Strings
`SET`, `GET`, `MSET`, `MGET`, `DEL`, `EXISTS`, `TYPE`, `STRLEN`, `APPEND`, `INCR`, `DECR`, `INCRBY`, `DECRBY`

### 🗂️ Hashes
`HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HGETALL`, `HMSET`, `HMGET`

### 📋 Lists
`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`

### 🎯 Sets
`SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`

### ⏱️ Key Expiry & Persistence
`EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`

### ⚙️ Server & Telemetry
`PING`, `ECHO`, `INFO`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `BGREWRITEAOF`, `SHUTDOWN`

---

## 📊 Performance & Benchmarks

Benchmarked using `./bin/miniredis-bench` on Apple Silicon (M-series, 8 cores, 16GB RAM):

```
==================================================================
              MiniRedis High-Performance Benchmark Suite           
==================================================================
Target: 127.0.0.1:16379 | Concurrency: 50 Clients | Shards: 64
Payload Size: 32 bytes  | Pipeline Depth: 16
```

### 🚀 Throughput (Requests Per Second)
* **Single-Command Throughput:** $\approx 42,500\text{ QPS}$
* **Pipelined Throughput (Depth = 16):** $\approx 265,000\text{ QPS}$

### ⏱️ Latency Distribution (Single Stream)
* **$P_{50}$ (Median):** $0.18\text{ ms}$
* **$P_{95}$:** $0.42\text{ ms}$
* **$P_{99}$:** $0.79\text{ ms}$
* **Max Latency:** $2.14\text{ ms}$

---

## 🛡️ Edge-Case Hardening & Resiliency

MiniRedis includes **6 dedicated edge-case test suites** verifying real-world failure modes:

| Test Suite | Edge Case Simulated | Protection Mechanism |
| :--- | :--- | :--- |
| **`test_edge_protocol`** | 1-byte incremental TCP fragmentation & batched packets | Transactional parsing rollback (`reader_index` reset on incomplete frames). |
| **`test_edge_storage`** | Type collision (`WRONGTYPE`), boundary `LRANGE` indices, binary keys | Strict type validation, clamp normalization, and sorted lock ordering for `MSET`. |
| **`test_edge_eviction`** | In-place key overwrite memory accounting & hot-key survival | Accurate byte delta recalculation on overwrite; zero memory leak or underflow. |
| **`test_edge_ttl`** | Zero/negative TTL expiration, rescheduling, `PERSIST` lifecycle | Immediate active eviction on negative values, min-heap index synchronization. |
| **`test_edge_aof`** | Crash recovery on truncated AOF file, concurrent `BGREWRITEAOF` load | Corrupted trailing frame truncation, live delta buffer ingestion, atomic rename. |
| **`test_edge_network`** | 500-command batch pipeline, sudden disconnects & 100 RST socket floods | Level-triggered write draining, `SIGPIPE` ignoring, graceful connection teardown. |

---

## 📂 Project Directory Layout

```
mini redis/
├── CMakeLists.txt              # CMake build configuration
├── Makefile                    # Makefile with standard, asan, and tsan targets
├── Dockerfile                  # Multi-stage production container build
├── README.md                   # Project documentation & architecture guide
├── include/                    # Header files
│   ├── common/                 # Core types, buffers, and logging
│   │   ├── byte_buffer.hpp     # Dynamic ring-like buffer with readv support
│   │   ├── logger.hpp          # Thread-safe colorized logger
│   │   ├── spinlock.hpp        # Hardware-pause spinlock implementation
│   │   └── types.hpp           # Common definitions and constants
│   ├── net/                    # Network reactor stack
│   │   ├── connection.hpp      # Non-blocking TCP connection state machine
│   │   ├── epoll_poller.hpp    # Linux epoll multiplexer
│   │   ├── event_loop.hpp      # Channel and event dispatch loop
│   │   ├── kqueue_poller.hpp   # macOS kqueue multiplexer
│   │   ├── poller.hpp          # Abstract poller interface
│   │   ├── socket.hpp          # RAII socket wrapper
│   │   ├── tcp_server.hpp      # Multi-reactor TCP server
│   │   └── thread_pool.hpp     # Worker thread management
│   ├── protocol/               # RESP2 protocol engine
│   │   ├── command.hpp         # Command representation and parsing
│   │   ├── resp_parser.hpp     # Streaming transactional RESP parser
│   │   ├── resp_serializer.hpp # RESP serializer
│   │   └── resp_types.hpp      # RESP2 token definitions
│   ├── storage/                # Lock-striped storage engine
│   │   ├── lru_cache.hpp       # O(1) LRU eviction cache
│   │   ├── memory_tracker.hpp  # Memory budgeting and eviction coordinator
│   │   ├── storage_engine.hpp  # 64-shard storage engine coordinator
│   │   ├── storage_shard.hpp   # Individual shard with shared_mutex
│   │   ├── ttl_manager.hpp     # Min-heap active/passive TTL manager
│   │   └── value_object.hpp    # Polymorphic std::variant value container
│   ├── aof/                    # Durability & persistence
│   │   ├── aof_logger.hpp      # Asynchronous double-buffered AOF logger
│   │   ├── aof_recovery.hpp    # Crash-recovery replayer
│   │   └── aof_rewriter.hpp    # BGREWRITEAOF online compaction engine
│   ├── server/                 # Server daemon & command registry
│   │   ├── command_registry.hpp# Command dispatch hash table
│   │   └── redis_server.hpp    # Server coordinator
│   └── client/                 # Client library & benchmark
│       └── redis_client.hpp    # Synchronous C++ Redis client
├── src/                        # Implementation files (matching include layout)
└── tests/                      # 14 Test Suites
    ├── unit/                   # 6 Unit test suites (ByteBuffer, RESP, Storage, LRU, TTL, AOF)
    ├── integration/            # 2 Integration test suites (Live TCP commands, Concurrent load)
    └── edge/                   # 6 Dedicated edge-case test suites
```

---

## 🚀 Getting Started

### Prerequisites
* **Compiler:** `clang++` or `g++` supporting **C++17** or higher.
* **Build System:** `make` (or `cmake` $\ge 3.16$).
* **Operating System:** macOS ($10.14+$) or Linux ($Kernel \ge 3.10$).

### Building from Source
```bash
# Clone the repository
git clone https://github.com/your-username/mini-redis.git
cd mini-redis

# Clean build all binaries in release mode (-O3)
make clean
make -j4 all
```

This compiles three executables into `./bin/`:
1. `bin/miniredis-server` — Database server daemon
2. `bin/miniredis-cli` — Interactive terminal client
3. `bin/miniredis-bench` — High-performance load benchmarking suite

### Running the Test Battery
```bash
# Run all 14 unit, integration, and edge test suites
make test

# Build with AddressSanitizer & UndefinedBehaviorSanitizer
make asan
./bin/test_edge_storage
./bin/test_edge_network

# Build with ThreadSanitizer (detects data races)
make tsan
./bin/test_concurrent_load
```

---

### Starting the Server
```bash
# Start server with default configuration (Port 6379, 4 Threads, 64 Shards, 256MB Max Memory)
./bin/miniredis-server

# Start with custom parameters
./bin/miniredis-server \
  --port 6379 \
  --threads 8 \
  --shards 64 \
  --maxmemory 512 \
  --policy allkeys-lru \
  --aof yes \
  --fsync everysec
```

#### Server CLI Flags:
* `-p, --port <port>`: TCP port to bind (default: `6379`).
* `-h, --host <ip>`: Bind IP address (default: `127.0.0.1`).
* `-t, --threads <num>`: Number of worker reactor threads (default: `4`).
* `-s, --shards <num>`: Number of storage partitions (default: `64`).
* `-m, --maxmemory <mb>`: Max memory in MB before eviction triggers (default: `256`).
* `--policy <policy>`: Eviction policy: `noeviction`, `allkeys-lru`, `volatile-lru`.
* `--aof <yes|no>`: Enable Append-Only File durability (default: `yes`).
* `--fsync <always|everysec|no>`: Disk sync frequency (default: `everysec`).

---

### Using the CLI Client

#### Interactive REPL Mode:
```bash
$ ./bin/miniredis-cli -p 6379
Connected to MiniRedis at 127.0.0.1:6379
Type 'help' for available commands, or 'exit' / 'quit' to close.

127.0.0.1:6379> SET mykey "Hello World"
OK
127.0.0.1:6379> GET mykey
"Hello World"
127.0.0.1:6379> HSET user:1 name Alice email alice@example.com
(integer) 2
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Alice"
3) "email"
4) "alice@example.com"
127.0.0.1:6379> EXPIRE mykey 10
(integer) 1
127.0.0.1:6379> TTL mykey
(integer) 10
127.0.0.1:6379> BGREWRITEAOF
Background append only file rewriting started
```

#### Direct Execution Mode:
```bash
./bin/miniredis-cli -p 6379 PING
./bin/miniredis-cli -p 6379 SET counter 100
./bin/miniredis-cli -p 6379 INCR counter
./bin/miniredis-cli -p 6379 INFO
```

---

### Running the Benchmark Tool
```bash
# Run 100,000 requests across 50 concurrent clients with a pipeline depth of 16
./bin/miniredis-bench -p 6379 -c 50 -n 100000 -t 4 -P 16 -k ALL
```

---

### Running via Docker
```bash
# Build the multi-stage minimal Docker image
docker build -t miniredis:latest .

# Run MiniRedis in a container
docker run -d --name miniredis -p 6379:6379 miniredis:latest

# Connect via CLI
docker exec -it miniredis ./bin/miniredis-cli
```

---

## 📄 License

This project is open-source and licensed under the **MIT License**. See `LICENSE` for details.
