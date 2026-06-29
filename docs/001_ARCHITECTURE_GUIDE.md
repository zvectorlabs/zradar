# zradar Architecture Guide

A contributor-facing overview of how the codebase is structured, how data flows, and where to make changes. Read this before touching anything in `crates/`.

---

## 1. Core Principles

- **Layered dependency flow:** dependencies point inward — transport → service → domain → storage trait. Nothing in the domain or service layer depends on a specific database or HTTP framework.
- **Async-first:** all I/O is async (Tokio). No blocking in async contexts.
- **Trait-based boundaries:** service logic depends on traits, not concrete implementations.
- **Single binary:** one `zradar-server` process handles OTLP ingestion (gRPC/HTTP) and the query/admin API. No microservice split.
- **PostgreSQL is the control plane only.** File registry, settings, retention policies, audit logs. All telemetry (spans, metrics, logs) lives in Parquet files.
- **WAL is mandatory.** Every ingest is durably appended to the WAL before gRPC OK is returned. Parquet writing happens asynchronously in the background.

---

## 2. Crate Map

```
crates/
  applications/
    zradar-server          ← single binary: wires everything together, runs gRPC + HTTP

  services/
    api                    ← Admin HTTP/gRPC: query, analytics, retention, settings, audit
    api-optel              ← OTLP gRPC/HTTP: ingestion guard chain, conventions pipeline, converter

  core/
    zradar-models          ← Span, Metric, LogRecord, Config — plain Rust structs, no framework deps
    zradar-traits          ← TelemetryWriter/Reader, FileListRepository, Authenticator, ServiceError
    zradar-parquet         ← WriteBuffer, FlushWorker, ParquetFileWriter/Reader, Compactor, FileMover
    zradar-retention       ← RetentionPolicy, CleanupJob, QueryEnforcer
    zradar-policy          ← PolicyEnforcer, UsageTracker, quota checks
    zradar-wal             ← append-only WAL segments, WalFlusher, WalJanitor, WalReplayer
    zradar-runtime         ← shared Tokio runtime setup, background job scheduling
    zradar-metrics         ← internal Prometheus metrics
    zradar-auth-config     ← static API key config, auth context

  plugins/
    zradar-plugin-postgres ← file_list, settings, retention, audit repositories (PostgreSQL)
    zradar-plugin-s3       ← S3BlockStorage for warm Parquet files
```

> **Storage direction:** the plugin layer is being simplified — Postgres and S3 will be wired directly into the service layer. See [ROADMAP.md](../ROADMAP.md).

---

## 3. Layering Rules

Each layer can only depend **downward**:

| Layer | Can depend on | Cannot depend on |
|-------|--------------|-----------------|
| Transport (`api`, `api-optel`) | Service traits, domain models | Each other's internals, plugin impls, sqlx |
| Service (logic inside `api`, `api-optel`) | Domain models, repository traits | axum, tonic, sqlx directly |
| Domain (`zradar-models`, `zradar-traits`) | Nothing | Everything |
| Repository (`zradar-plugin-*`) | Domain models | Service, transport |

**Wiring happens only in `zradar-server`.** It is the only crate that imports both service logic and plugin implementations to assemble the running system.

---

## 4. Write Path

```
OTLP client
  └─ gRPC :4317  (or HTTP :4318)
      └─ api-optel ingestion guard chain
          ① Authenticator       bearer token → RequestContext { tenant_id, project_id }
          ② CircuitBreaker      disk / memory / queue thresholds
          ③ PolicyEnforcer      byte-rate quota check
          ④ ProjectRateLimiter  per-project token bucket
          ⑤ OtlpConverter       protobuf → Vec<Span> | Vec<Metric> | Vec<LogRecord>
                └─ Conventions pipeline  (spans only — see §5)
      └─ TelemetryWriter (trait)
          └─ WalTelemetryWriter  ← MANDATORY: every ingest appended + fsynced before OK
              client receives gRPC OK here
              ↓ background
              WalFlusher → WriteBuffer → FlushWorker → ParquetFileWriter
                  → .parquet on local disk  +  PostgreSQL file_list entry
                  → FileMover: local → S3 (after configurable delay)
```

---

## 5. Conventions Pipeline

`crates/services/api-optel/src/conventions/` maps OTLP span attributes to `Span` model fields. Each convention implements the `AttributeConvention` trait (`apply(&AttrView, &mut Span)`) and runs in priority order. Most-specific namespaces run first to win field conflicts.

Current conventions (priority order, defined in `conventions/mod.rs: default_conventions()`):

| # | Convention | Namespace |
|---|------------|-----------|
| 1 | `OpenInferenceConvention` | OpenInference |
| 2 | `GuardrailsConvention` | `rail.*`, `action.*` |
| 3 | `AgentConvention` | `agent.*`, `user_id`, `session_id` |
| 4 | `VertexConvention` | `gcp.vertex.agent.*` |
| 5 | `LlmConvention` | `llm.*` (model, usage, cost) |
| 6 | `GenAiV1_29Convention` | OTel GenAI 1.29 — `gen_ai.usage.*`, `gen_ai.response.*` |
| 7 | `NatConvention` | `nat.*` |
| 8 | `AiqConvention` | AIQ framework |
| 9 | `PromptConvention` | `prompt.*` |
| 10 | `SamplingParamsConvention` | `sampling.*` |
| 11 | `ToolConvention` | `tool.*` |
| 12 | `ResourceConvention` | `resource.*` |
| 13 | `GenAiLegacyConvention` | legacy `gen_ai.*` pre-1.29 |

**To add a new convention** (e.g., MCP):
1. Create `conventions/mcp.rs` implementing `AttributeConvention`
2. Register at the right priority in `default_conventions()` in `conventions/mod.rs`
3. Add new `Span` fields to `zradar-models/src/span.rs` + migration in `zradar-plugin-postgres/migrations/`

---

## 6. Read Path

```
Admin client
  └─ HTTP/gRPC :8081 (query) or :8082 (admin mutations)
      └─ api guard chain
          ① AdminAuthorizer    bearer token → AdminAuth + capability check
          ② PolicyEnforcer     read quota check
          ③ QueryEnforcer      clamp time_range to retention window
      └─ FileListRepository   PostgreSQL — list Parquet files covering the time range
      └─ ParquetFileReader
          local files  → direct read
          S3 files     → MemoryCache → DiskCache → S3BlockStorage → download
          DataFusion ListingTable → parallel row-group scan
      └─ PaginatedResponse<TraceSummary | Span | LogRecord | Metric>
```

---

## 7. Port Architecture

| Port | Protocol | Purpose |
|------|----------|---------|
| `4317` | gRPC | OTLP ingestion — traces, metrics, logs |
| `4318` | HTTP | OTLP/HTTP ingestion (alternate) |
| `8081` | gRPC + gRPC-Web | Query API — read-only telemetry |
| `8082` | gRPC + gRPC-Web | Admin API — mutations and config |

Query (8081) and Admin (8082) are on separate ports for network-level isolation. Health (`/health`) and Prometheus (`/metrics`) are served as HTTP fallback routes on the gRPC ports.

---

## 8. Error Handling

- Library crates: `thiserror` structured error types
- Service layer: `ServiceError` from `zradar-traits` — transport-agnostic
- Transport layer maps `ServiceError → HTTP status` or `ServiceError → tonic::Status` via `From` impls in each transport crate
- Binary (`zradar-server`): `anyhow` for startup/wiring errors only
- Never panic in library code

---

## 9. Adding a Feature — Checklist

**New span field (e.g. `mcp_tool_name`):**
- [ ] Add field to `zradar-models/src/span.rs`
- [ ] Add migration in `zradar-plugin-postgres/migrations/`
- [ ] Create `conventions/mcp.rs` implementing `AttributeConvention`
- [ ] Register in `default_conventions()` in `conventions/mod.rs`
- [ ] Expose via query API in `crates/services/api/src/telemetry/`

**New repository operation:**
- [ ] Add method to trait in `zradar-traits/src/`
- [ ] Implement in `zradar-plugin-postgres/`
- [ ] Call from service layer via the trait only — never import sqlx in service logic

**New background job:**
- [ ] Implement in the appropriate core crate (`zradar-parquet`, `zradar-retention`, etc.)
- [ ] Schedule in `zradar-runtime` or `zradar-server`
