# Multi-stage Docker build for MiniRedis
FROM ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    clang \
    make \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source code and build
COPY . .
RUN make clean && make all -j$(nproc)

# Final lightweight runtime container
FROM ubuntu:22.04 AS runner

WORKDIR /app

# Copy compiled binaries from builder stage
COPY --from=builder /app/bin/miniredis-server /usr/local/bin/miniredis-server
COPY --from=builder /app/bin/miniredis-cli /usr/local/bin/miniredis-cli
COPY --from=builder /app/bin/miniredis-bench /usr/local/bin/miniredis-bench

EXPOSE 6379

ENTRYPOINT ["miniredis-server"]
CMD ["-p", "6379", "-t", "4", "-s", "64", "-m", "256", "--policy", "allkeys-lru", "--aof", "yes"]
