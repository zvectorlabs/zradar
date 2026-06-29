# Production image for zradar-server (OTLP gRPC + Admin HTTP API).
#
# Build from zradar/zradar:
#   docker build -t zradar .

FROM rust:1.90-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    g++ \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto

RUN sed -i '/"test_functional",/d' Cargo.toml

ENV SQLX_OFFLINE=true

RUN cargo build --release --bin zradar

FROM debian:bookworm-slim AS server

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 zradar

RUN mkdir -p /app/config /app/scripts /app/data/trace-batches /app/data/wal \
    && chown -R zradar:zradar /app

WORKDIR /app

COPY --from=builder /app/target/release/zradar /usr/local/bin/zradar
COPY config.toml.example /app/config/config.toml.example
COPY scripts /app/scripts/

USER zradar

EXPOSE 4317 8081

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8081/health || exit 1

ENV RUST_LOG=info,zradar=debug

CMD ["zradar"]
