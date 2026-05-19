# zradar Architecture Guide

**System Prompt for Agents:** This document defines the architectural constraints and patterns for the `zradar` codebase. All code changes must align with these principles.

---

## 1. Core Principles

- **Plugin-Based Architecture:** The core system defines *traits* (interfaces), and plugins provide *implementations*. The core never depends on specific plugins.
- **Async-First:** All I/O is async (Tokio). No blocking operations in async contexts.
- **Service-Oriented:** Business logic resides in `services`, isolated from transport (HTTP/gRPC) and storage details.
- **Strict Layering:** Dependencies flow *inwards* towards the core.

---

## 2. Crate Structure

| Layer | Directory | Purpose | Dependencies Allowed |
|-------|-----------|---------|----------------------|
| **Applications** | `crates/applications` | Entry points (Server, Worker). Wires plugins to core. | All layers |
| **Services** | `crates/services` | Business logic (e.g., `api_keys`, `projects`). | `core` |
| **Plugins** | `crates/plugins` | Concrete implementations (e.g., `postgres`, `s3`). | `core` |
| **Core** | `crates/core` | Shared traits, models, and errors. | None (Leaf nodes) |

### Key Core Crates
- `zradar-traits`: Defines abstract interfaces (e.g., `Storage`, `Auth`).
- `zradar-models`: Shared data structures (DTOs, DB entities).
- `zradar-plugins`: Plugin registry and loading mechanisms.

---

## 3. Development Rules for Agents

1.  **Dependency Direction:**
    - `services` MUST NOT depend on `plugins`.
    - `core` MUST NOT depend on anything else.
    - Only `applications` can depend on specific `plugins` to wire them up.

2.  **Adding Features:**
    - Define the behavior in `zradar-traits` (if new capability).
    - Implement the logic in `crates/services`.
    - If storage/external access is needed, use the trait, NOT a specific DB client.
    - Implement the trait in `crates/plugins` (e.g., `zradar-plugin-postgres`).

3.  **Error Handling:**
    - Use `thiserror` for library crates (`core`, `services`, `plugins`).
    - Use `anyhow` only in `applications` (binaries).
    - Errors must be propagated, not panicked.

---

## 4. Data Flow

1.  **Request:** Enters via `applications` (e.g., HTTP request to `zradar-server`).
2.  **Handler:** Dispatched to a handler in `crates/services`.
3.  **Logic:** Service executes business logic.
4.  **Abstraction:** Service calls a `trait` method (e.g., `UserRepository::get`).
5.  **Implementation:** The configured `plugin` (e.g., `zradar-plugin-postgres`) executes the actual operation.
