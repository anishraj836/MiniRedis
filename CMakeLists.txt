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
    src/server/command_rewrite.cpp
    src/server/redis_server.cpp
    src/server/commands_string.cpp
    src/server/commands_hash.cpp
    src/server/commands_list.cpp
    src/server/commands_set.cpp
    src/server/commands_ttl.cpp
    src/server/commands_server.cpp
    src/client/redis_client.cpp
)

find_package(Threads REQUIRED)

add_library(miniredis_core STATIC ${CORE_SOURCES})
target_link_libraries(miniredis_core PUBLIC Threads::Threads)

# Executables
add_executable(miniredis-server src/server/main_server.cpp)
target_link_libraries(miniredis-server PRIVATE miniredis_core Threads::Threads)

add_executable(miniredis-cli src/client/main_cli.cpp)
target_link_libraries(miniredis-cli PRIVATE miniredis_core Threads::Threads)

add_executable(miniredis-bench src/client/main_bench.cpp)
target_link_libraries(miniredis-bench PRIVATE miniredis_core Threads::Threads)

# Unit & Integration Tests
enable_testing()

set(TEST_SOURCES
    tests/unit/test_byte_buffer.cpp
    tests/unit/test_resp_parser.cpp
    tests/unit/test_storage_engine.cpp
    tests/unit/test_lru_eviction.cpp
    tests/unit/test_ttl_expiration.cpp
    tests/unit/test_aof_recovery.cpp
    tests/integration/test_server_commands.cpp
    tests/integration/test_concurrent_load.cpp
    tests/edge/test_edge_protocol.cpp
    tests/edge/test_edge_storage.cpp
    tests/edge/test_edge_eviction.cpp
    tests/edge/test_edge_ttl.cpp
    tests/edge/test_edge_aof.cpp
    tests/edge/test_edge_network.cpp
    tests/edge/test_regressions.cpp
)

foreach(test_src ${TEST_SOURCES})
    get_filename_component(test_name ${test_src} NAME_WE)
    add_executable(${test_name} ${test_src})
    target_link_libraries(${test_name} PRIVATE miniredis_core Threads::Threads)
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
