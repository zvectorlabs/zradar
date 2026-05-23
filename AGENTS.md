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
make db-gui
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

- PostgreSQL: control plane data, organizations, projects, API keys, audit logs, and queues by default.
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
