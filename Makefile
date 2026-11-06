CXX ?= clang++
CXXFLAGS ?= -std=c++17 -O3 -Wall -Wextra -Wpedantic -pthread -Iinclude
LDFLAGS ?= -pthread
# Emit .d files so that editing a header rebuilds every object that includes it.
DEPFLAGS = -MMD -MP

SRC_COMMON = src/common/byte_buffer.cpp src/common/logger.cpp
SRC_NET = src/net/socket.cpp src/net/event_loop.cpp src/net/poller.cpp src/net/kqueue_poller.cpp src/net/epoll_poller.cpp src/net/poll_poller.cpp src/net/connection.cpp src/net/thread_pool.cpp src/net/tcp_server.cpp
SRC_PROTOCOL = src/protocol/resp_parser.cpp src/protocol/resp_serializer.cpp src/protocol/command.cpp
SRC_STORAGE = src/storage/storage_shard.cpp src/storage/storage_engine.cpp src/storage/lru_cache.cpp src/storage/ttl_manager.cpp src/storage/memory_tracker.cpp
SRC_AOF = src/aof/aof_logger.cpp src/aof/aof_rewriter.cpp src/aof/aof_recovery.cpp
SRC_SERVER = src/server/command_registry.cpp src/server/command_rewrite.cpp src/server/redis_server.cpp src/server/commands_string.cpp src/server/commands_hash.cpp src/server/commands_list.cpp src/server/commands_set.cpp src/server/commands_ttl.cpp src/server/commands_server.cpp
SRC_CLIENT = src/client/redis_client.cpp

CORE_SRCS = $(SRC_COMMON) $(SRC_NET) $(SRC_PROTOCOL) $(SRC_STORAGE) $(SRC_AOF) $(SRC_SERVER) $(SRC_CLIENT)
CORE_OBJS = $(CORE_SRCS:.cpp=.o)

BIN_DIR = bin

TARGET_SERVER = $(BIN_DIR)/miniredis-server
TARGET_CLI = $(BIN_DIR)/miniredis-cli
TARGET_BENCH = $(BIN_DIR)/miniredis-bench

# Test suites, in the order `make test` runs them: tests/<dir>/<name>.cpp -> bin/<name>
TEST_SRCS = tests/unit/test_byte_buffer.cpp \
            tests/unit/test_resp_parser.cpp \
            tests/unit/test_storage_engine.cpp \
            tests/unit/test_lru_eviction.cpp \
            tests/unit/test_ttl_expiration.cpp \
            tests/unit/test_aof_recovery.cpp \
            tests/integration/test_server_commands.cpp \
            tests/integration/test_concurrent_load.cpp \
            tests/edge/test_edge_protocol.cpp \
            tests/edge/test_edge_storage.cpp \
            tests/edge/test_edge_eviction.cpp \
            tests/edge/test_edge_ttl.cpp \
            tests/edge/test_edge_aof.cpp \
            tests/edge/test_edge_network.cpp \
            tests/edge/test_regressions.cpp
TEST_BINS = $(addprefix $(BIN_DIR)/,$(basename $(notdir $(TEST_SRCS))))

all: $(TARGET_SERVER) $(TARGET_CLI) $(TARGET_BENCH)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -c $< -o $@

$(TARGET_SERVER): src/server/main_server.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -o $@ src/server/main_server.cpp $(CORE_OBJS) $(LDFLAGS)

$(TARGET_CLI): src/client/main_cli.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -o $@ src/client/main_cli.cpp $(CORE_OBJS) $(LDFLAGS)

$(TARGET_BENCH): src/client/main_bench.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -o $@ src/client/main_bench.cpp $(CORE_OBJS) $(LDFLAGS)

$(BIN_DIR)/test_%: tests/unit/test_%.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -o $@ $< $(CORE_OBJS) $(LDFLAGS)

$(BIN_DIR)/test_%: tests/integration/test_%.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -o $@ $< $(CORE_OBJS) $(LDFLAGS)

$(BIN_DIR)/test_%: tests/edge/test_%.cpp $(CORE_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -o $@ $< $(CORE_OBJS) $(LDFLAGS)

tests: $(TEST_BINS)

test: tests
	@echo "\n================= RUNNING ALL UNIT, INTEGRATION & EDGE TESTS ================="
	@set -e; for t in $(TEST_BINS); do echo "./$$t"; ./$$t; done
	@echo "================= ALL $(words $(TEST_BINS)) TEST SUITES PASSED =================\n"

# Sanitizer runs: rebuild everything with instrumentation and run the suites.
# They overwrite the regular objects, so run `make clean` before a normal build.
SAN_FLAGS = -std=c++17 -O1 -g -fno-omit-frame-pointer -Wall -Wextra -Iinclude -pthread

asan:
	$(MAKE) clean
	$(MAKE) test CXXFLAGS="$(SAN_FLAGS) -fsanitize=address,undefined -fno-sanitize-recover=undefined" \
	             LDFLAGS="-fsanitize=address,undefined -pthread"

tsan:
	$(MAKE) clean
	$(MAKE) test CXXFLAGS="$(SAN_FLAGS) -fsanitize=thread" LDFLAGS="-fsanitize=thread -pthread"

clean:
	rm -rf $(BIN_DIR) src/*/*.o src/*/*.d tests/*/*.o *.aof *.temp

.PHONY: all tests test asan tsan clean

-include $(CORE_OBJS:.o=.d) $(addsuffix .d,$(TEST_BINS) $(TARGET_SERVER) $(TARGET_CLI) $(TARGET_BENCH))
