CXX ?= clang++
CXXFLAGS ?= -std=c++17 -O3 -Wall -Wextra -Wpedantic -pthread -Iinclude
LDFLAGS ?= -pthread

SRC_COMMON = src/common/byte_buffer.cpp src/common/logger.cpp
SRC_NET = src/net/socket.cpp src/net/event_loop.cpp src/net/poller.cpp src/net/kqueue_poller.cpp src/net/epoll_poller.cpp src/net/poll_poller.cpp src/net/connection.cpp src/net/thread_pool.cpp src/net/tcp_server.cpp
SRC_PROTOCOL = src/protocol/resp_parser.cpp src/protocol/resp_serializer.cpp src/protocol/command.cpp
SRC_STORAGE = src/storage/storage_shard.cpp src/storage/storage_engine.cpp src/storage/lru_cache.cpp src/storage/ttl_manager.cpp src/storage/memory_tracker.cpp
SRC_AOF = src/aof/aof_logger.cpp src/aof/aof_rewriter.cpp src/aof/aof_recovery.cpp
SRC_SERVER = src/server/command_registry.cpp src/server/redis_server.cpp src/server/commands_string.cpp src/server/commands_hash.cpp src/server/commands_list.cpp src/server/commands_set.cpp src/server/commands_ttl.cpp src/server/commands_server.cpp
SRC_CLIENT = src/client/redis_client.cpp

CORE_SRCS = $(SRC_COMMON) $(SRC_NET) $(SRC_PROTOCOL) $(SRC_STORAGE) $(SRC_AOF) $(SRC_SERVER) $(SRC_CLIENT)
CORE_OBJS = $(CORE_SRCS:.cpp=.o)

BIN_DIR = bin

TARGET_SERVER = $(BIN_DIR)/miniredis-server
TARGET_CLI = $(BIN_DIR)/miniredis-cli
TARGET_BENCH = $(BIN_DIR)/miniredis-bench

TEST_BYTE_BUFFER = $(BIN_DIR)/test_byte_buffer
TEST_RESP_PARSER = $(BIN_DIR)/test_resp_parser
TEST_STORAGE = $(BIN_DIR)/test_storage_engine
TEST_LRU = $(BIN_DIR)/test_lru_eviction
TEST_TTL = $(BIN_DIR)/test_ttl_expiration
TEST_AOF = $(BIN_DIR)/test_aof_recovery
TEST_INTEGRATION_SERVER = $(BIN_DIR)/test_server_commands
TEST_INTEGRATION_LOAD = $(BIN_DIR)/test_concurrent_load

TEST_EDGE_PROTO = $(BIN_DIR)/test_edge_protocol
TEST_EDGE_STORAGE = $(BIN_DIR)/test_edge_storage
TEST_EDGE_EVICTION = $(BIN_DIR)/test_edge_eviction
TEST_EDGE_TTL = $(BIN_DIR)/test_edge_ttl
TEST_EDGE_AOF = $(BIN_DIR)/test_edge_aof
TEST_EDGE_NET = $(BIN_DIR)/test_edge_network

all: $(TARGET_SERVER) $(TARGET_CLI) $(TARGET_BENCH)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TARGET_SERVER): src/server/main_server.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ src/server/main_server.cpp $(CORE_OBJS) $(LDFLAGS)

$(TARGET_CLI): src/client/main_cli.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ src/client/main_cli.cpp $(CORE_OBJS) $(LDFLAGS)

$(TARGET_BENCH): src/client/main_bench.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ src/client/main_bench.cpp $(CORE_OBJS) $(LDFLAGS)

tests: $(BIN_DIR) $(CORE_OBJS) \
       $(TEST_BYTE_BUFFER) $(TEST_RESP_PARSER) $(TEST_STORAGE) \
       $(TEST_LRU) $(TEST_TTL) $(TEST_AOF) \
       $(TEST_INTEGRATION_SERVER) $(TEST_INTEGRATION_LOAD) \
       $(TEST_EDGE_PROTO) $(TEST_EDGE_STORAGE) $(TEST_EDGE_EVICTION) \
       $(TEST_EDGE_TTL) $(TEST_EDGE_AOF) $(TEST_EDGE_NET)

$(TEST_BYTE_BUFFER): tests/unit/test_byte_buffer.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/unit/test_byte_buffer.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_RESP_PARSER): tests/unit/test_resp_parser.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/unit/test_resp_parser.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_STORAGE): tests/unit/test_storage_engine.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/unit/test_storage_engine.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_LRU): tests/unit/test_lru_eviction.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/unit/test_lru_eviction.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_TTL): tests/unit/test_ttl_expiration.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/unit/test_ttl_expiration.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_AOF): tests/unit/test_aof_recovery.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/unit/test_aof_recovery.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_INTEGRATION_SERVER): tests/integration/test_server_commands.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/integration/test_server_commands.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_INTEGRATION_LOAD): tests/integration/test_concurrent_load.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/integration/test_concurrent_load.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_EDGE_PROTO): tests/edge/test_edge_protocol.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/edge/test_edge_protocol.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_EDGE_STORAGE): tests/edge/test_edge_storage.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/edge/test_edge_storage.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_EDGE_EVICTION): tests/edge/test_edge_eviction.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/edge/test_edge_eviction.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_EDGE_TTL): tests/edge/test_edge_ttl.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/edge/test_edge_ttl.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_EDGE_AOF): tests/edge/test_edge_aof.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/edge/test_edge_aof.cpp $(CORE_OBJS) $(LDFLAGS)

$(TEST_EDGE_NET): tests/edge/test_edge_network.cpp $(CORE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ tests/edge/test_edge_network.cpp $(CORE_OBJS) $(LDFLAGS)

test: tests
	@echo "\n================= RUNNING ALL UNIT, INTEGRATION & EDGE TESTS ================="
	./$(TEST_BYTE_BUFFER)
	./$(TEST_RESP_PARSER)
	./$(TEST_STORAGE)
	./$(TEST_LRU)
	./$(TEST_TTL)
	./$(TEST_AOF)
	./$(TEST_INTEGRATION_SERVER)
	./$(TEST_INTEGRATION_LOAD)
	./$(TEST_EDGE_PROTO)
	./$(TEST_EDGE_STORAGE)
	./$(TEST_EDGE_EVICTION)
	./$(TEST_EDGE_TTL)
	./$(TEST_EDGE_AOF)
	./$(TEST_EDGE_NET)
	@echo "================= ALL 14 TEST SUITES PASSED FLAWLESSLY! =================\n"

clean:
	rm -rf $(BIN_DIR) src/*/*.o tests/*/*.o *.aof *.temp

.PHONY: all tests test clean
