cmake_minimum_required(VERSION 3.15)
project(MiniRedis VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Optimization flags
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    add_compile_options(-O3 -Wall -Wextra -Wpedantic -pthread)
endif()

include_directories(include)

# Core Library Sources
set(CORE_SOURCES
    src/common/byte_buffer.cpp
    src/common/logger.cpp
    src/net/socket.cpp
    src/net/event_loop.cpp
    src/net/poller.cpp
    src/net/kqueue_poller.cpp
    src/net/epoll_poller.cpp
    src/net/poll_poller.cpp
    src/net/connection.cpp
    src/net/thread_pool.cpp
    src/net/tcp_server.cpp
    src/protocol/resp_parser.cpp
    src/protocol/resp_serializer.cpp
    src/protocol/command.cpp
    src/storage/storage_shard.cpp
    src/storage/storage_engine.cpp
    src/storage/lru_cache.cpp
    src/storage/ttl_manager.cpp
    src/storage/memory_tracker.cpp
    src/aof/aof_logger.cpp
    src/aof/aof_rewriter.cpp
    src/aof/aof_recovery.cpp
    src/server/command_registry.cpp
    src/server/redis_server.cpp
    src/server/commands_string.cpp
    src/server/commands_hash.cpp
    src/server/commands_list.cpp
    src/server/commands_set.cpp
    src/server/commands_ttl.cpp
    src/server/commands_server.cpp
    src/client/redis_client.cpp
)

add_library(miniredis_core STATIC ${CORE_SOURCES})
target_link_libraries(miniredis_core PUBLIC Threads::Threads)

find_package(Threads REQUIRED)

# Executables
add_executable(miniredis-server src/server/main_server.cpp)
target_link_libraries(miniredis-server PRIVATE miniredis_core Threads::Threads)

add_executable(miniredis-cli src/client/main_cli.cpp)
target_link_libraries(miniredis-cli PRIVATE miniredis_core Threads::Threads)

add_executable(miniredis-bench src/client/main_bench.cpp)
target_link_libraries(miniredis-bench PRIVATE miniredis_core Threads::Threads)

# Unit & Integration Tests
enable_testing()

add_executable(test_byte_buffer tests/unit/test_byte_buffer.cpp)
target_link_libraries(test_byte_buffer PRIVATE miniredis_core Threads::Threads)
add_test(NAME test_byte_buffer COMMAND test_byte_buffer)

add_executable(test_resp_parser tests/unit/test_resp_parser.cpp)
target_link_libraries(test_resp_parser PRIVATE miniredis_core Threads::Threads)
add_test(NAME test_resp_parser COMMAND test_resp_parser)

add_executable(test_storage_engine tests/unit/test_storage_engine.cpp)
target_link_libraries(test_storage_engine PRIVATE miniredis_core Threads::Threads)
add_test(NAME test_storage_engine COMMAND test_storage_engine)

add_executable(test_lru_eviction tests/unit/test_lru_eviction.cpp)
target_link_libraries(test_lru_eviction PRIVATE miniredis_core Threads::Threads)
add_test(NAME test_lru_eviction COMMAND test_lru_eviction)

add_executable(test_ttl_expiration tests/unit/test_ttl_expiration.cpp)
target_link_libraries(test_ttl_expiration PRIVATE miniredis_core Threads::Threads)
add_test(NAME test_ttl_expiration COMMAND test_ttl_expiration)

add_executable(test_aof_recovery tests/unit/test_aof_recovery.cpp)
target_link_libraries(test_aof_recovery PRIVATE miniredis_core Threads::Threads)
add_test(NAME test_aof_recovery COMMAND test_aof_recovery)

add_executable(test_server_commands tests/integration/test_server_commands.cpp)
target_link_libraries(test_server_commands PRIVATE miniredis_core Threads::Threads)
add_test(NAME test_server_commands COMMAND test_server_commands)

add_executable(test_concurrent_load tests/integration/test_concurrent_load.cpp)
target_link_libraries(test_concurrent_load PRIVATE miniredis_core Threads::Threads)
add_test(NAME test_concurrent_load COMMAND test_concurrent_load)
