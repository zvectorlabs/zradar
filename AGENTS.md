# AGENTS.md

## Purpose

This file is the operating guide for AI agents and contributors working in this repository. It is self-contained and applies to the entire `zradar` project unless a more specific `AGENTS.md` exists in a subdirectory.

## Non-Negotiable Rule: Use `make`

Always use `make` targets for development lifecycle commands.

Do not call lifecycle tools directly when a `make` target exists. This includes `cargo`, `docker-compose`, migrations, SQLx cache generation, formatting, linting, testing, local runs, production-like runs, deployment, logs, shells, and cleanup.

Use the repository `Makefile` as the source of truth for dev workflows because it wires hooks, Docker orchestration, SQLx cache generation, migrations, and project-specific defaults together.

## Quick Reference

### Discovery

```bash
make help
```

### Development

```bash
make dev
make dev-logs
make start
make stop
make restart
make status
make logs
make logs-server
make health
```

### Testing

```bash
make test
make functional_tests
make functional_tests_fast
make functional_tests_fast TEST_NAME=test_name
```

### Quality

```bash
make fmt
make check
make lint
make fix
make hook
```

### Database and SQLx

```bash
make migrate
make sqlx-prepare
make clean-sqlx
make db-shell
```

### Builds and Runtime

```bash
make build-prod
make prod
make prod-stop
make release
make run
```

### Deployment

```bash
make deploy
make deploy-stop
```

### Release and Version Bumps

Use `just release-publish` — never edit `Cargo.toml` or `VERSION` directly.

```bash
just show-version                    # print current version
just release-publish patch           # bump patch (0.8.1 → 0.8.2), tag, push
just release-publish minor           # bump minor (0.8.1 → 0.9.0), tag, push
just release-publish major           # bump major (0.8.1 → 1.0.0), tag, push
just release-publish 1.2.3           # set exact version, tag, push
```

`just release-publish` performs the full release cycle in one shot:
1. Bumps `VERSION` and `[workspace.package].version` in `Cargo.toml`
2. Runs `cargo check` to verify the workspace compiles
3. Creates a release commit (`chore(release): v<N>`) and an annotated git tag
4. Pushes the commit + tag to origin — this triggers CI to build and publish release artifacts

**Requirements:** working tree must be clean before running (commit or stash first).

To preview without pushing: `SKIP_PUSH=1 just release-publish minor`
To dry-run entirely: `DRY_RUN=1 just release-publish minor`

### Cleanup

```bash
make clean
make clean-all
```

`make clean-all` deletes the local `data/` directory. Treat it as destructive and do not run it unless explicitly requested.

## Expected Workflow

1. Inspect the relevant files and existing patterns before changing code.
2. Prefer the smallest change that solves the problem.
3. Run the narrowest useful validation through `make`.
4. For Rust code changes, normally run:

```bash
make fmt
make check
make lint
make test
```

5. For behavior involving Docker services, use `make dev`, `make status`, `make logs`, and `make health`.
6. For database query changes, regenerate SQLx metadata with `make sqlx-prepare` or `make clean-sqlx` as appropriate.
7. Ensure hooks are installed or updated with `make hook` when working on commits.

## Repository Architecture

`zradar` is a Rust service-oriented telemetry platform. Major responsibilities include:

- `crates/applications/zradar-server`: single OTLP gRPC + Admin HTTP server binary.
- `crates/services/api`: Admin HTTP routes (telemetry queries, analytics, settings, retention, audit).
- `crates/services/api-optel`: OTLP gRPC services, circuit breaker, project rate limiter.
- `crates/core/zradar-models`: shared data structures (Span, Metric, LogRecord, Config).
- `crates/core/zradar-traits`: trait abstractions (TelemetryWriter/Reader, FileListRepository, Authenticator).
- `crates/core/zradar-parquet`: Parquet writer/reader, write buffer, flush worker, compactor, file mover, retention job, recovery.
- `crates/core/zradar-retention`: retention policies + cleanup job + query enforcer.
- `crates/plugins/zradar-plugin-postgres`: Postgres-backed file_list, settings, retention, and audit repositories.
- `crates/plugins/zradar-plugin-s3`: S3 block storage for warm Parquet files.

Storage responsibilities:

- PostgreSQL: control plane data — file_list, stream_stats, settings, retention policies, and audit logs.
- Parquet: telemetry data such as traces, spans, metrics, and logs.

## Rust Best Practices

### Async and I/O

- All file, network, and storage I/O should be async.
- Never block the Tokio executor.
- Use `spawn_blocking` for CPU-bound work such as distance calculations, compression, or index building.
- Never hold locks across `.await` points.

### Concurrency

Prefer concurrency patterns in this order:

1. Lock-free structures for high-frequency reads and writes.
2. Actor ownership for single-writer state.
3. Sharding for high-contention partitionable state.
4. Copy-on-write for read-heavy, rarely updated state.
5. Async locks only when they are truly required.

Preferred structures:

- `DashMap` for concurrent maps.
- `ArcSwap` for read-heavy shared state.
- `crossbeam::SegQueue` for producer-consumer queues.
- `parking_lot::RwLock` for fast synchronous locks.
- `tokio::sync::RwLock` for async locks.

### Memory and Performance

- Use `Bytes` or `BytesMut` for buffers.
- Slice rather than clone where practical.
- Pre-allocate in hot paths.
- Avoid allocations inside tight loops.
- Batch operations where possible.
- Minimize lock scope.
- Use SIMD for vector operations when appropriate, with scalar fallback.

### Traits and Types

- Async-compatible traits should be `Send + Sync`.
- Use `Box<dyn Trait>` for runtime plugins.
- Use generics or `impl Trait` for hot paths.
- Use `#[async_trait]` when async trait methods are needed.
- Use newtypes for IDs to prevent accidental mixing, such as organization IDs, project IDs, API key IDs, user IDs, trace IDs, and span IDs.

### Error Handling

- Use structured error types with `thiserror` in libraries.
- Add context when propagating failures.
- Do not panic in library code.
- Return clear validation, storage, authorization, and not-found errors.
- Use `ServiceError` (in `zradar-traits`) for all service-layer errors. It is transport-agnostic — no dependency on axum or tonic.
- Each transport layer (HTTP, gRPC) maps `ServiceError` to its own wire format via `From` impls. Do not implement transport traits on `ServiceError` directly in `zradar-traits`.

## Architecture Design Principles

### Layered Architecture

The codebase follows a strict 4-layer architecture. Each layer can only depend **downward**:

```
Layer 1: Transport (gRPC / HTTP)   ← axum, tonic, proto messages, JSON DTOs
Layer 2: Service (Business Logic)  ← trait-based, transport-agnostic
Layer 3: Domain Models             ← plain Rust structs, no framework deps
Layer 4: Repository (Storage)      ← trait-based, implementation-swappable
```

| Layer | Can Depend On | Cannot Depend On | Crate |
|-------|--------------|------------------|-------|
| **Transport (gRPC)** | Service traits, Proto messages, Domain models | HTTP types, axum, JSON DTOs | `api` (grpc submodule) |
| **Transport (HTTP)** | Service traits, JSON DTOs, Domain models | Proto messages, tonic | `api` (http submodule) |
| **Service** | Domain models, Repository traits | axum, tonic, proto, JSON, sqlx | `api` (service impls) or dedicated crate |
| **Domain Models** | Nothing (leaf) | Everything | `zradar-models`, `zradar-traits` |
| **Repository** | Domain models | Service, Transport | `zradar-traits` (traits), `zradar-plugin-*` (impls) |

### Trait-Based Service Design

- All business logic lives behind `#[async_trait]` service traits defined in `zradar-traits/src/services/`.
- Service traits: `TelemetryQueryService`, `AnalyticsQueryService`, `RetentionService`, `PolicyAdminService`, `AuditQueryService`, `SettingsAdminService`.
- Both gRPC and HTTP transport handlers depend on `Arc<dyn ServiceTrait>`, never on concrete structs.
- Service trait methods take `&self` plus domain-level input types and return `Result<DomainType, ServiceError>`.
- Service traits do not take transport-specific types (`HeaderMap`, `MetadataMap`, `Query<T>`, proto messages).

### Auth Abstraction

- `AuthContext` (in `zradar-traits`) is the transport-agnostic auth context passed to service methods.
- Each transport extracts auth from its own mechanism (HTTP headers → axum extractor, gRPC metadata → tonic interceptor) and produces the same `AuthContext`.
- Service methods receive `AuthContext` and call `auth.require(Capability::X)` for authorization. They never inspect transport headers directly.

### Model Conversion Boundaries

Three model spaces exist with clear `From`/`Into` boundaries:

- **Proto messages** (generated by `tonic-build`) ↔ **Domain models** (in `zradar-traits`/`zradar-models`)
- **HTTP DTOs** (serde `Deserialize`) ↔ **Domain models**
- **Storage models** (from repository traits) ↔ **Domain models**

Conversion rules:
- Proto ↔ Domain conversions live in `api/src/grpc/conversions.rs`.
- HTTP DTO ↔ Domain conversions live in `api/src/http/conversions.rs` (or inline via `From` impls).
- Storage ↔ Domain conversions live in the repository implementation crate.
- Never pass proto messages or HTTP DTOs below the transport layer.
- Never return storage models above the service layer without mapping.

### Port Architecture

| Port | Protocol | Purpose | Isolation |
|------|----------|---------|-----------|
| `4317` | gRPC | OTLP Ingestion (traces, metrics, logs) | Ingest-only, high throughput |
| `4318` | HTTP | OTLP/HTTP Ingestion (optional) | Ingest-only |
| `8081` | gRPC + gRPC-Web | Query API — read-only telemetry | Read-only, firewallable |
| `8082` | gRPC + gRPC-Web | Admin API — mutations & config | Admin-only, restricted access |

- Query and Admin are on separate ports for network-level isolation.
- `tonic-web` is enabled on both gRPC ports for browser clients (Connect-Web).
- Health (`/health`, `/health/live`, `/health/ready`) and Prometheus (`/metrics`) are served as HTTP fallback routes on the same gRPC ports.


### Multi-Tenancy and Security

- Always enforce organization and project boundaries.
- Authenticate before authorization.
- Apply tenant filters to data queries.
- Use PostgreSQL transactions for control plane mutations.
- Avoid multi-step mutations that can leave inconsistent state.
- Do not hardcode secrets, tokens, API keys, passwords, or production endpoints.

### Testing

- Use `#[tokio::test]` for async tests.
- Use `tempfile` for filesystem tests.
- Use property-based testing for invariants when appropriate.
- Benchmark performance-critical paths against a baseline.
- Add or update tests for behavior changes.

### Style

- Keep imports ordered as standard library, external crates, then internal modules.
- Keep line length around 100 characters where practical.
- Use `todo!()` for intentionally unfinished code rather than silent placeholders.
- Keep Clippy warning-free.
- Preserve existing module boundaries and naming conventions.

## Commit and Hook Expectations

Use `make hook` to install or refresh repository hooks.

Commit messages must follow Conventional Commits:

```text
<type>(<scope>): <subject>
```

Accepted types include:

- `feat`
- `fix`
- `docs`
- `style`
- `refactor`
- `perf`
- `test`
- `chore`
- `build`
- `ci`

Example:

```text
feat(auth): add social login
```

## Agent Safety Rules

- Do not run destructive commands unless explicitly requested.
- Do not use `make clean-all` unless explicitly requested and the data deletion risk is acknowledged.
- Do not bypass `make` by calling direct lifecycle commands when a Make target exists.
- Do not introduce new dependencies without checking existing project conventions and dependency files.
- Do not modify generated or cached files unless the task requires it.
- Do not make broad rewrites when a targeted patch is sufficient.
- Keep changes consistent with `CODING_GUIDELINES.md`.

## Before Finishing

For code changes, report which `make` validations were run. If validation was not run, state why.

Preferred final checks:

```bash
make fmt
make check
make lint
make test
```

@CODING_GUIDELINES.md
