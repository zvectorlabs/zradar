# zradar

Agent Tracing & LLM Observability Platform with high-performance OpenTelemetry ingestion and ClickHouse backend.

## Overview

zradar is an **agent observability platform** designed for tracing AI agents, LLM workflows, and agent-based applications. Built on OpenTelemetry Protocol (OTLP), it provides comprehensive observability for agent sessions, LLM calls, tool executions, and complex multi-step workflows. It features a scalable architecture with asynchronous processing, multi-tenancy, and comprehensive RBAC.

## Features

### Agent Observability
- **Agent Tracing** - Track agent sessions, workflows, and execution paths
- **LLM Observability** - Monitor model calls, token usage, costs, and performance
- **Tool Execution Tracking** - Instrument and analyze tool/function calls
- **Workflow Visualization** - Tree, timeline, and graph views of agent execution
- **Quality Scoring** - Custom evaluations and quality metrics for agents and LLM calls
- **Cost Analysis** - Track LLM costs, token usage, and budget per agent/project

### Platform Features
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

## Agent Observability

zradar is purpose-built for observing AI agents and LLM applications. It supports:

### Agent Span Types
- **AGENT** - Agent execution spans (conversational agents, task agents, etc.)
- **GENERATION** - LLM generation calls (chat completions, text generation)
- **TOOL** - Tool/function calls executed by agents
- **CHAIN** - Chain of operations (LangChain, LlamaIndex chains)
- **RETRIEVER** - RAG retrieval operations
- **EVALUATOR** - Evaluation operations
- **EMBEDDING** - Embedding generation
- **GUARDRAIL** - Guardrail/safety checks

### Key Metrics Tracked
- **Agent Performance**: Session count, success rate, average latency per agent
- **LLM Usage**: Model calls, token usage (prompt/completion/total), costs per model
- **Tool Usage**: Tool call frequency, success rate, execution time
- **Quality Scores**: Accuracy, relevance, latency, custom evaluations
- **Cost Analysis**: LLM costs, trends, budget tracking, cost per agent
- **Error Analysis**: LLM errors, tool errors, agent errors (by type)

### LLM-Specific Attributes
zradar tracks rich LLM metadata including:
- Model information (vendor, model name, temperature, max_tokens)
- Token usage (prompt, completion, total)
- Cost tracking (input, output, total costs)
- Response metadata (request/response IDs, finish reasons)
- Embedding dimensions and usage

See [examples/README.md](examples/README.md) for code examples showing agent and LLM instrumentation.

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

### Development Hooks

To ensure code quality and consistent commit messages, please install the git hooks:

```bash
# Install hooks
cp scripts/hooks/* .git/hooks/
chmod +x .git/hooks/*
```

These hooks will check:
- Code formatting (`cargo fmt`)
- Clippy warnings (`cargo clippy`)
- Compilation (`cargo check`)
- Commit message format (Conventional Commits)


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
