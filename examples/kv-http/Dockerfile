# Build stage
FROM rust:latest AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire workspace (needed for path dependencies)
COPY . .

# Build the kv-http example in release mode
WORKDIR /app/examples/kv-http
RUN cargo build --release

# Runtime stage - use trixie to match GLIBC version from rust:latest
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3t64 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/examples/kv-http/target/release/kv-http /usr/local/bin/kv-http

# Create data directory
RUN mkdir -p /data

ENV DATA_DIR=/data
ENV RUST_LOG=info

EXPOSE 5001 8001

CMD ["kv-http"]
