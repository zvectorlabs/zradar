# AGENTS.md

## Purpose

This file is the operating guide for AI agents and contributors working in this repository. It is self-contained and applies to the entire `zradar` project unless a more specific `AGENTS.md` exists in a subdirectory.

## Non-Negotiable Rule: Use `just`

Always use `just` recipes for development lifecycle commands.

Do not call lifecycle tools directly when a `just` recipe exists. This includes `cargo`, `docker compose`, migrations, SQLx cache generation, formatting, linting, testing, local runs, builds, and cleanup.

Use `just` (see `justfile`) as the source of truth for dev workflows — it wires Docker orchestration, SQLx cache generation, migrations, hooks, and project-specific defaults together.

## Quick Reference

```bash
just          # list all available recipes
```

### Setup (once per clone)

```bash
just bootstrap   # install cargo tools + git hooks
just doctor      # verify environment, auto-fix installable tools
just hook        # install / refresh pre-commit and commit-msg hooks
```

### Development

```bash
just dev          # start Postgres + zradar with hot reload
just dev-logs     # start + follow logs
just stop         # stop all services
just restart      # stop then start
just status       # container status + live health check
just health       # hit /health and /health/ready
just logs         # tail all container logs
just logs-server  # tail zradar server logs only
just shell        # shell in running zradar container
just db-shell     # psql shell against running Postgres
```

### Testing

```bash
just test                          # unit tests
just functional-tests              # E2E, fresh Docker environment
just functional-tests-fast         # E2E reusing running dev stack
just functional-tests-fast <name>  # single named functional test
just test-all                      # unit + functional
```

### Code Quality

```bash
just fmt     # cargo fmt
just check   # cargo check --all-targets
just lint    # cargo clippy -D warnings (zero warnings required)
just fix     # apply cargo fix + clippy --fix
```

### Framework Examples (requires `just dev` running)

```bash
just example <framework>                # run one example against local zradar
just example-test <framework>           # run + validate spans in zradar
just example-test-all                   # run all 10 frameworks end-to-end
just example-update-snapshot <fw>       # regenerate expected_spans.json snapshot
just sdk-check                          # check PyPI/npm for SDK version drift
```

Frameworks: `langchain` · `openai-agents` · `openai` · `pydantic-ai` · `crewai` · `llamaindex` · `anthropic` · `google-adk` · `vercel-ai-sdk` · `mastra`

All examples use mock LLMs — no real API key required. Default dev key: `zk_dev_local`.

### Database and SQLx

```bash
just migrate       # run pending migrations
just sqlx-prepare  # regenerate .sqlx/ offline query cache
just clean-sqlx    # delete + regenerate .sqlx/ cache
```

Run `just sqlx-prepare` after any change to `sqlx::query!` macros or migration files. Commit `.sqlx/` to keep CI builds offline-capable.

### Builds and Release

```bash
just build-release           # optimised binary → target/release/zradar
just run                     # run locally (requires DATABASE_URL)
just build-prod              # build production Docker images
just show-version            # print current version
just version-bump [patch|minor|major]   # bump VERSION + Cargo.toml
just release-publish [patch|minor|major] # bump → commit → tag → push
```

### Cleanup

```bash
just clean      # stop containers + volumes (keeps ./data/)
just clean-all  # stop + delete ./data/ — DESTRUCTIVE, prompts for confirmation
```

`just clean-all` deletes all telemetry data. Do not run unless explicitly requested.

## Expected Workflow

1. Inspect relevant files and existing patterns before making changes.
2. Prefer the smallest change that solves the problem.
3. Run the narrowest useful validation through `just`.
4. For Rust code changes, run:

```bash
just fmt
just check
just lint
just test
```

5. For Docker service behaviour, use `just dev`, `just status`, `just logs`, `just health`.
6. After schema changes, regenerate SQLx metadata: `just sqlx-prepare` or `just clean-sqlx`.
7. Hooks are installed by `just bootstrap` and refreshed by `just hook`.

## Key Documentation

| Doc | What it covers |
|-----|---------------|
| `docs/004_QUICKSTART.md` | Zero-to-first-trace in 5 steps |
| `docs/001_ARCHITECTURE_GUIDE.md` | Crate map, write/read path, conventions pipeline, layering rules |
| `docs/003_architecture-diagrams.md` | Mermaid diagrams for all major flows |
| `docs/005_DEVELOPER_WORKFLOWS.md` | Full `just` recipe reference with descriptions |
| `docs/006_CONFIGURATION.md` | All `config.toml` fields and environment variable overrides |
| `CONTRIBUTING.md` | How to contribute, commit style, RFC process, contributor ladder |
| `ROADMAP.md` | Direction items: MCP, UI, Kubernetes, white-glove onboarding |

## Repository Architecture

`zradar` is a Rust service-oriented telemetry platform. A single binary serves OTLP ingestion and the Admin query API.

```
crates/
  applications/zradar-server    ← single binary: wires everything together
  services/api                  ← Admin HTTP/gRPC: query, analytics, retention, settings, audit
  services/api-optel            ← OTLP gRPC/HTTP: ingestion guard chain + conventions pipeline
  core/zradar-models            ← Span, Metric, LogRecord, Config — plain Rust structs
  core/zradar-traits            ← TelemetryWriter/Reader, FileListRepository, Authenticator
  core/zradar-parquet           ← WriteBuffer, FlushWorker, ParquetFileWriter/Reader, FileMover
  core/zradar-retention         ← RetentionPolicy, CleanupJob, QueryEnforcer
  core/zradar-policy            ← PolicyEnforcer, UsageTracker, quota checks
  core/zradar-wal               ← append-only WAL — mandatory, fsynced before gRPC OK
  core/zradar-metrics           ← internal Prometheus metrics
  core/zradar-runtime           ← Tokio runtime setup, background job scheduling
  core/zradar-auth-config       ← static API key config, auth context
  plugins/zradar-plugin-postgres ← file_list, settings, retention, audit (PostgreSQL)
  plugins/zradar-plugin-s3       ← S3BlockStorage for warm Parquet files
```

Storage split:
- **PostgreSQL**: control plane — file_list, stream_stats, settings, retention policies, audit logs.
- **Parquet** (local disk → S3): all telemetry — spans, metrics, logs.

Write path: OTLP → guard chain (auth → circuit breaker → quota → rate limit → converter) → WAL (fsync) → background flush → Parquet.

Layering rule: dependencies point inward only. Transport (`api`, `api-optel`) → Service logic → Domain models (`zradar-models`, `zradar-traits`) → Repository (`zradar-plugin-*`). Wiring happens only in `zradar-server`.

## Rust Best Practices

### Async and I/O

- All file, network, and storage I/O must be async.
- Never block the Tokio executor.
- Use `spawn_blocking` for CPU-bound work (compression, index building).
- Never hold locks across `.await` points.

### Concurrency (prefer in order)

1. Lock-free — `DashMap`, `ArcSwap`
2. Actor — single-writer ownership
3. Sharding — high-contention partitionable state
4. Copy-on-write — read-heavy, rare updates
5. Async locks — `tokio::sync::RwLock` only when required

### Error Handling

- Library crates: `thiserror` structured errors.
- Service layer: `ServiceError` from `zradar-traits` (transport-agnostic).
- Transport layer maps `ServiceError → HTTP status` or `ServiceError → tonic::Status`.
- Binary: `anyhow` for startup/wiring errors only. Never panic in library code.

### Multi-Tenancy

- Always enforce org/project boundaries on every query.
- Authenticate before authorize.
- Use PostgreSQL transactions for control plane mutations.
- Never hardcode secrets, tokens, or production endpoints.

### Testing

- Async unit tests: `#[tokio::test]`.
- Filesystem tests: `tempfile`.
- Property invariants: `proptest`.
- Performance: `criterion`.

## Commit and Hook Expectations

Use `just hook` to install or refresh hooks.

### Commit message format (enforced by `commit-msg` hook)

```
<type>(<scope>): <subject>          ← max 70 characters
                                    ← blank line
- what changed and why              ← bullet points only
- second point if needed            ← max 5 bullets
```

Accepted types: `feat` `fix` `docs` `style` `refactor` `perf` `test` `chore` `build` `ci`

Good example:
```
feat(ingest): add MCP span type detection

- maps mcp.tool.name and mcp.server.name to span fields
- registers McpConvention at priority 3 in default_conventions()
```

### Pre-commit hook (runs on every commit, in order)

1. `just fmt` — formatting
2. `just lint` — clippy zero warnings
3. `just check` — compilation
4. `cargo deny check` — skipped if `deny.toml` is absent or `cargo-deny` not installed
5. Migration safety — blocks `DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN TYPE`, `DROP TABLE`, `ADD NOT NULL` without `DEFAULT`
6. Code deny — blocks `dbg!()` in non-test files, hardcoded secret literals

## Agent Safety Rules

- Do not run destructive commands unless explicitly requested.
- Do not use `just clean-all` unless explicitly requested and the data deletion risk is acknowledged.
- Do not bypass `just` by calling direct lifecycle commands when a recipe exists.
- Do not introduce new dependencies without checking existing project conventions and `Cargo.toml`.
- Do not modify generated or cached files (`.sqlx/`, `Cargo.lock`) unless the task requires it.
- Do not make broad rewrites when a targeted patch is sufficient.
- Keep changes consistent with `CODING_GUIDELINES.md`.

## Before Finishing

For code changes, report which `just` validations were run. If skipped, state why.

Preferred final checks:

```bash
just fmt
just check
just lint
just test
```

@CODING_GUIDELINES.md
