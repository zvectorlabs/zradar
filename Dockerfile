# Multi-stage build for zradar (layered architecture)
# Builds both server and worker binaries

# ============================================================================
# Builder stage - Compiles all binaries
# ============================================================================
FROM rust:1.90-slim AS builder

# Install dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations

# Remove test_functional from workspace (not needed for production build)
RUN sed -i '/"test_functional",/d' Cargo.toml

# Enable SQLx offline mode (compile without database)
ENV SQLX_OFFLINE=true

# Build both binaries for release
RUN cargo build --release --bin zradar --bin zradar-worker

# ============================================================================
# Server stage - Ingestion tier (NO workers)
# ============================================================================
FROM debian:bookworm-slim AS server

# Install runtime dependencies (including curl for health checks)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 zradar

# Create directories
RUN mkdir -p /app/config /app/scripts /app/data/trace-batches && chown -R zradar:zradar /app

WORKDIR /app

# Copy server binary from builder
COPY --from=builder /app/target/release/zradar /usr/local/bin/zradar

# Copy config and scripts
COPY config.toml.example /app/config/config.toml.example
COPY scripts /app/scripts/

# Switch to non-root user
USER zradar

# Expose ports
EXPOSE 4317 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Set environment
ENV RUST_LOG=info,zradar=debug

# Run server (ingestion only, no workers)
CMD ["zradar"]

# ============================================================================
# Worker stage - Processing tier
# ============================================================================
FROM debian:bookworm-slim AS worker

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 zradar

# Create directories
RUN mkdir -p /app/data/trace-batches && chown -R zradar:zradar /app

WORKDIR /app

# Copy worker binary from builder
COPY --from=builder /app/target/release/zradar-worker /usr/local/bin/zradar-worker

# Switch to non-root user
USER zradar

# Set environment
ENV RUST_LOG=info,zradar=debug
ENV WORKER_COUNT=8

# Run worker
CMD ["zradar-worker"]

