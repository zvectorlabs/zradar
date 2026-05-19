# Rust Coding Guidelines for zradar

**Version:** 1.0  
**Status:** Approved  
**Applies to:** zradar server, worker, and all crates

---

## 1. Async Patterns

### 1.1 All I/O Must Be Async

Every file, network, or storage operation uses `async fn`. Never block the tokio executor.

**CPU-bound work:** Offload to `spawn_blocking` (distance calculations, index building, compression).

### 1.2 Lock Discipline

Never hold locks across `.await` points - this causes deadlocks.

```
CORRECT:
    data = lock.read().clone()
    drop(lock)
    await storage.write(data)

WRONG:
    guard = lock.write()
    await storage.write(guard)  // Deadlock!
```

---

## 2. Concurrency Patterns

### 2.1 Pattern Selection (in order of preference)

| Priority | Pattern | When to Use |
|----------|---------|-------------|
| 1st | Lock-free | High-frequency reads/writes |
| 2nd | Actor | Single-writer ownership |
| 3rd | Sharding | High contention, partitionable data |
| 4th | Copy-on-Write | Read-heavy, rare updates |
| 5th | Async locks | Must hold across await |

### 2.2 Recommended Structures

| Use Case | Structure |
|----------|-----------|
| Concurrent map | `DashMap` |
| Read-heavy state | `ArcSwap` |
| Producer-consumer | `crossbeam::SegQueue` |
| Fast sync lock | `parking_lot::RwLock` |
| Async lock | `tokio::sync::RwLock` |

### 2.3 zradar Architecture

```
zradar
├── zvradar-server                   // REST API (actix-web)
├── zvradar-worker                   // Background processing
├── zvradar-control                  // Control plane logic
│   ├── Organizations                // Multi-tenant management
│   ├── Projects                     // Project isolation
│   ├── API Keys                     // Authentication
│   └── Permissions                  // RBAC
├── zvradar-otlp                     // OTLP ingest (tonic gRPC)
├── zvradar-ingestor                 // Batch processing
├── zvradar-storage                  // PostgreSQL metadata + Parquet telemetry
├── zvradar-auth                     // Authentication/authorization
└── zvradar-models                   // Shared data models
```

**Principle:** Service-oriented architecture with clear boundaries. PostgreSQL for control plane and metadata, Parquet for telemetry data.

---

## 3. Memory & Performance

### 3.1 Zero-Copy

- Use `Bytes` crate for buffers
- Slice instead of clone
- Pre-allocate in hot paths

### 3.2 Allocation Strategy

| Data Type | Allocation |
|-----------|------------|
| Fixed small (< 1KB) | Stack |
| Dynamic/large | Heap with `Vec` |
| Shared | `Arc<T>` (never `Rc`) |
| Buffers | `Bytes` / `BytesMut` |

### 3.3 Hot Path Rules

- No allocations in tight loops
- Use `#[inline]` for small, frequent functions
- SIMD for vector operations (with scalar fallback)
- Batch operations (one WAL write for N vectors)

---

## 4. Trait Design

### 4.1 Thread Safety

All traits must be `Send + Sync` for async compatibility.

```
TRAIT StorageBackend: Send + Sync
    async read(key) -> Bytes
    async write(key, data)
    async delete(key)
```

### 4.2 Polymorphism Strategy

| Need | Approach |
|------|----------|
| Runtime plugins | `Box<dyn Trait>` |
| Hot path performance | Generics / `impl Trait` |
| Async methods | `#[async_trait]` |

### 4.3 Default Implementations

Provide defaults where sensible to reduce boilerplate.

---

## 5. Error Handling

### 5.1 Error Types

- Library errors: `thiserror` with structured types
- Context: `.context("message")` for propagation
- Never panic in library code

### 5.2 Error Code Ranges

| Range | Domain |
|-------|--------|
| 1xxx | Not found |
| 2xxx | Validation |
| 3xxx | Storage |
| 4xxx | Partition |
| 5xxx | Index |

---

## 6. Type Safety

### 6.1 Newtypes for IDs

Wrap primitives to prevent mixing:
- `OrganizationId(Uuid)`
- `ProjectId(Uuid)` 
- `ApiKeyId(Uuid)`
- `UserId(Uuid)`
- `TraceId(String)` // OpenTelemetry format
- `SpanId(String)` // OpenTelemetry format

### 6.2 Construction Patterns

- **Simple:** Constructor function
- **Complex:** Builder pattern with validation
- **State:** Enum state machines

---

## 7. Project-Specific Rules

### 7.1 Multi-Tenancy Isolation

**Always enforce organization/project boundaries:**

```
FOR each_request:
    user = authenticate(api_key)
    project = authorize(user, project_id)
    IF NOT user.has_permission(project, action):
        RETURN Forbidden
    data = query_with_tenant_filter(org_id, project_id)
```

### 7.2 Batch Processing

- Collect traces in batches before telemetry persistence
- Use PostgreSQL transactions for control plane mutations
- Implement graceful degradation for storage failures

### 7.3 Storage Patterns

- PostgreSQL: Control plane (organizations, projects, API keys, audit logs)
- Parquet: Telemetry data (traces, spans, metrics, logs)
- Redis: Rate limiting, caching, session management
- Atomic operations: Use database transactions, avoid multi-step mutations

---

## 8. Testing

| Test Type | Framework | Use For |
|-----------|-----------|---------|
| Async unit | `#[tokio::test]` | All async code |
| Filesystem | `tempfile` | Storage tests |
| Invariants | `proptest` | Property-based |
| Performance | `criterion` | Benchmarks |

---

## 9. Crate Preferences

| Purpose | Crate |
|---------|-------|
| Lock-free map | `dashmap` |
| Lock-free swap | `arc-swap` |
| Lock-free queue | `crossbeam` |
| Fast locks | `parking_lot` |
| Async runtime | `tokio` |
| Errors | `thiserror` |
| Serialization | `serde`, `bincode` |
| Buffers | `bytes` |
| Embedded KV | `redb` |
| io_uring | `tokio-uring` |

---

## 10. Code Style

- `cargo fmt` before commit
- `cargo clippy` zero warnings
- Max line length: 100 characters
- Import order: std → external → internal
- `todo!()` for unfinished code, never silent placeholders

---

## 11. Performance Checklist

Before merging performance-critical code:

- [ ] No allocations in hot loops
- [ ] Filter pushdown applied
- [ ] Batch operations where possible
- [ ] Lock scope minimized
- [ ] SIMD for vector ops
- [ ] Benchmark vs baseline
- [ ] No blocking in async context

