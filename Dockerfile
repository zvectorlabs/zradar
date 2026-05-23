# Single-stage build for zradar (single-tier OTLP server).
# The historical "worker" tier was removed; ingestion is direct-write Parquet.

# ============================================================================
# Builder
# ============================================================================
FROM rust:1.90-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN sed -i '/"test_functional",/d' Cargo.toml

ENV SQLX_OFFLINE=true

RUN cargo build --release --bin zradar

# ============================================================================
# Runtime
# ============================================================================
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

EXPOSE 4317 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

ENV RUST_LOG=info,zradar=debug

CMD ["zradar"]
