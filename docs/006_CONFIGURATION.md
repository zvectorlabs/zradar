# Configuration Reference

zradar is configured through a TOML file (default: `config.toml` in the working directory) with environment variable overrides for deployment-time values.

Start from the example:

```bash
cp config.toml.example config.toml
```

---

## Environment variable overrides

These take precedence over `config.toml` at runtime. Set them in `.env` or your container environment.

| Variable | Overrides | Example |
|----------|-----------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgres://zradar:pass@localhost:5432/zradar` |
| `OTLP_PORT` | `otlp_port` | `4317` |
| `QUERY_API_PORT` | `query_api_port` (Admin API) | `8081` |
| `STORAGE_TYPE` | `[ingestor].storage_type` | `local` or `s3` |
| `AUTO_MIGRATE` | `[migrations].auto_migrate` | `true` |
| `RUST_LOG` | Log level | `info,zradar=debug` |
| `RUST_BACKTRACE` | Stack traces on panic | `1` |

Copy `env.example` to `.env` for local development:

```bash
cp env.example .env
```

---

## Top-level fields

```toml
otlp_port      = 4317   # gRPC OTLP ingestion port
query_api_port = 8081   # Admin HTTP/gRPC query + mutation API port
batch_size     = 1000   # max spans per write batch
batch_timeout_seconds = 10
```

---

## [postgres]

PostgreSQL is used for the control plane only — file registry, settings, retention policies, and audit logs. All telemetry lives in Parquet files.

```toml
[postgres]
max_connections = 20   # connection pool size
```

The connection string is always supplied via `DATABASE_URL`:

```bash
# Docker Compose (inside container)
DATABASE_URL=postgres://zradar:dev_password@postgres:5432/zradar

# Local app talking to Docker Compose Postgres
DATABASE_URL=postgres://zradar:dev_password@localhost:5432/zradar

# Native (no Docker)
DATABASE_URL=postgres://zradar:password@localhost:5432/zradar
```

---

## [auth]

Controls how OTLP and Admin API requests are authenticated.

```toml
[auth]
# mode = "standalone"           # default — validate tokens against [[api_keys]]
# otlp_require_api_key = false  # set to false for open ingest (dev/testing only)
```

### [[api_keys]]

Define one entry per project. Every OTLP export and Admin API request must carry the key as a Bearer token.

```toml
[[api_keys]]
key          = "zk_live_changeme"   # replace with a secure random value
tenant_id    = "my-org"
workspace_id = "00000000-0000-0000-0000-000000000000"
project_id   = "my-project"
name         = "default"

# Add more keys for additional projects:
# [[api_keys]]
# key        = "zk_live_another"
# tenant_id  = "my-org"
# project_id = "another-project"
# name       = "another"
```

The default dev key in `config.server.toml` (used by Docker Compose) is `zk_dev_local`.

---

## [ingestor]

Controls where Parquet files are written.

```toml
[ingestor]
storage_type = "local"   # "local" (dev) or "s3" (production)
```

### Local storage

```toml
[ingestor.storage.local]
path = "./data/trace-batches"   # host path (or container path)
```

### S3 storage

```toml
[ingestor.storage.s3]
bucket = "my-traces-bucket"
region = "us-east-1"
# prefix = "zradar/"          # optional key prefix
```

S3 credentials come from the standard AWS credential chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / IAM role).

### Parquet write settings

These tune the write buffer and compaction. For most deployments the defaults are fine.

```toml
[ingestor.storage.parquet]
write_buffer_enabled             = false   # buffer spans in memory before flushing
write_buffer_size_bytes          = 8388608 # 8 MiB — flush when buffer hits this
write_buffer_flush_interval_secs = 30      # flush at least every N seconds
fsync_before_rename              = true    # durability guarantee (WAL handles this)
compaction_enabled               = false   # merge small Parquet files periodically
memory_cache_enabled             = false   # cache hot Parquet pages in memory
```

> **WAL note:** Every ingest is durably written to the WAL and fsynced before gRPC OK is returned regardless of these settings. The write buffer only affects how quickly spans land in Parquet files.

---

## [migrations]

```toml
[migrations]
auto_migrate = true   # run pending SQL migrations on startup
```

Set `AUTO_MIGRATE=false` (or `auto_migrate = false`) in production if you manage migrations separately.

---

## Port summary

| Port | Protocol | Purpose |
|------|----------|---------|
| `4317` | gRPC | OTLP ingestion — traces, metrics, logs |
| `4318` | HTTP | OTLP/HTTP ingestion (alternate) |
| `8081` | gRPC + HTTP | Admin API — query, analytics, settings, retention |

Health endpoints (`/health`, `/health/live`, `/health/ready`) and Prometheus metrics (`/metrics`) are served on port `8081`.

---

## Minimal production config

```toml
otlp_port      = 4317
query_api_port = 8081

[postgres]
max_connections = 20

[auth]
[[api_keys]]
key          = "zk_live_your_secure_key"
tenant_id    = "your-org"
workspace_id = "00000000-0000-0000-0000-000000000000"
project_id   = "your-project"
name         = "production"

[ingestor]
storage_type = "s3"

[ingestor.storage.s3]
bucket = "your-traces-bucket"
region = "us-east-1"

[migrations]
auto_migrate = false
```

With environment:

```bash
DATABASE_URL=postgres://zradar:strongpassword@your-db-host:5432/zradar
RUST_LOG=info,zradar=info
```
