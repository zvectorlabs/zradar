# zradar

High-performance OpenTelemetry ingestion service with ClickHouse backend.

## Overview

zradar is a telemetry ingestion service that receives OTLP (OpenTelemetry Protocol) data and stores it in ClickHouse for fast querying and analysis. It features a scalable architecture with asynchronous processing, multi-tenancy, and comprehensive RBAC.

## Features

- **Standard OTLP Protocol** - Works with any OpenTelemetry client
- **Asynchronous Processing** - Job queue with separate worker processes
- **Dual Database Architecture** - PostgreSQL (control) + ClickHouse (data)
- **Multi-tenancy** - Organization + Project hierarchy
- **Scalable** - PostgreSQL queue (up to 50 workers) or Hybrid queue (1000+ workers)
- **Block Storage** - Local filesystem or S3 for raw telemetry data

## Quick Start

### Prerequisites

- Rust 1.90+
- PostgreSQL 17+ (Main DB)
- ClickHouse 23.0+ (Optional)
- Redis (Optional)
- S3 or S3 compatible Block Storage (Optional)

### Docker Compose (Recommended)

```bash
# Start all services
docker-compose up -d

# Scale workers
docker-compose up -d --scale zradar-worker=4
```


## Configuration

See `config.toml.example` for full configuration options. Key settings:

```toml
[ingestor]
queue_type = "postgres"  # or "hybrid" for high-scale
storage_type = "local"   # or "s3" for production

[workers]
num_workers = 8
```

## Architecture

zradar uses a **plugin-based architecture** that separates core abstractions from implementations, enabling backend swapping without code changes.

### Plugin System

- **Core Traits** (`zradar-traits`) - Define interfaces for storage, queues, telemetry, and repositories
- **Plugin System** (`zradar-plugins`) - Runtime plugin registry and loader with configuration-driven initialization
- **Plugin Implementations** - Available plugins:
  - **postgres** (built-in) - Control plane, job queue, and basic storage
  - **clickhouse** - High-performance telemetry storage and analytics
  - **s3** - Block storage for trace batches
  - **redis** - Distributed cache and hybrid queue
  - **local** - Local filesystem storage

Plugins are configured in `config/plugins.toml` and loaded dynamically at runtime.

### Data Flow

```
OTLP Clients → Server → Job Queue → Workers → Plugins → Backends
```

The server handles ingestion and enqueues jobs. Workers process jobs asynchronously and route to configured plugins (ClickHouse, S3, etc.).

## Deployment

### Queue Types

- **PostgreSQL Queue** (default): Simple deployment, up to 50 workers, ~10K jobs/sec
- **Hybrid Queue** (Redis+PG): High-scale, 1000+ workers, ~500K jobs/sec

### Scaling

- **Small**: 1 server, 1-3 workers, PostgreSQL queue
- **Medium**: 2-3 servers, 10-50 workers, PostgreSQL queue, S3 storage
- **Large**: 5+ servers, 100-1000 workers, Hybrid queue, S3 storage

## Development

```bash
# Run server
RUST_LOG=debug cargo run --bin zradar-server

# Run worker
RUST_LOG=debug cargo run --bin zradar-worker

# Run tests
cargo test
```

## Documentation

- [Configuration Example](config.toml.example)
- [API Documentation](http://localhost:8080/swagger-ui/) (when server running)
- See `crates/` directory for code documentation

## License

This project uses dual licensing:

- **Apache 2.0**: Main codebase (see [LICENSE](LICENSE))
- **Enterprise License**: Code in `ee/` directories (see [LICENSE-EE](LICENSE-EE))

## Status

🚧 **Under active development** - Not yet production-ready
