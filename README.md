# zradar

[![CI](https://github.com/zvectorlabs/zradar/actions/workflows/ci.yml/badge.svg)](https://github.com/zvectorlabs/zradar/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**OpenTelemetry-native observability built specifically for AI agents, LLM applications, and multi-step workflows.**

zradar helps developers understand what their agents are doing, why they fail, how much they cost, and where latency appears. It ingests standard OTLP telemetry and stores high-volume traces, metrics, and logs in a **Parquet-first architecture** built for fast analytical queries at scale—without the massive cost of standard OLTP databases.

---

## Mission

AI observability should be affordable and complete — from a single agent on a laptop to a multi-agent cluster on Kubernetes processing millions of tool calls a day.

zradar is the observability layer that scales with you: one OpenTelemetry standard, zero vendor lock-in, every use case covered — LLM completions, MCP tool calls, agentic workflows, RAG pipelines, and Kubernetes-deployed services. We believe full-stack visibility should never be the thing that breaks your budget.

See [ROADMAP.md](ROADMAP.md) for what's coming next.

---

## ⚡ Why choose zradar?

Traditional APM tools aren't built for non-deterministic LLM chains. Zradar focuses on the specific pain points of modern AI development:

- **AI-Native Context:** Connect agent sessions, LLM generation, tool usage, RAG retrievers, and evaluations in a unified trace timeline.
- **Drastically Lower Costs:** Store massive telemetry volumes cheaply in Parquet while PostgreSQL handles lightweight metadata and control-plane state.
- **Control LLM Spend:** Track prompt tokens, completion tokens, latency, and estimated cost across models, agents, and projects. 
- **No Vendor Lock-In:** Use the OpenTelemetry standards you already know. Send OTLP data directly from your app—no proprietary zradar SDKs required.
- **Local First:** Spin up the stack locally in seconds with Docker. Debug locally with Parquet files before moving to S3-compatible object storage.

---

## 🚀 Quick Start

The fastest way to try zradar is via Docker Compose. Create a `docker-compose.yml` file:

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: zradar
      POSTGRES_PASSWORD: password
      POSTGRES_DB: zradar
    ports:
      - "5432:5432"

  zradar:
    image: ghcr.io/zvectorlabs/zradar:latest
    environment:
      DATABASE_URL: postgres://zradar:password@postgres:5432/zradar
      AUTO_MIGRATE: true
    ports:
      - "4317:4317" # OTLP gRPC
      - "8081:8081" # Admin API
    depends_on:
      - postgres
```

Run it with:
```bash
docker-compose up -d
```

- **OTLP gRPC Ingestion:** `localhost:4317` (Send your traces, metrics, and logs here)
- **Admin HTTP API:** `http://localhost:8081` (Query analytics and settings)
- **Swagger / OpenAPI UI:** `http://localhost:8081/swagger-ui/`

For a step-by-step walkthrough — including how to send your first trace and query it back — see **[docs/004_QUICKSTART.md](docs/004_QUICKSTART.md)**.

---

## 🧠 What you can observe

zradar correlates logs, metrics, and traces together into the shape of AI applications.

```mermaid
flowchart TD
    user["User request"] --> session["Agent session"]
    session --> plan["Planning step"]
    session --> rag["Retrieval / RAG"]
    session --> tool["Tool execution"]
    session --> llm["LLM generation"]
    session --> eval["Evaluation / guardrail"]
    plan --> timeline["Unified trace timeline"]
    rag --> timeline
    tool --> timeline
    llm --> timeline
    eval --> timeline
```

---

## 📐 Design Principles

zradar is built from the ground up to solve the operational headaches of modern AI systems without introducing new ones. Our core engineering principles are:

- **Cost-Effective Scaling:** Telemetry data is loud and heavy. By storing high-volume logs, metrics, and traces directly in **Parquet** files (on local disk or S3), you bypass the exorbitant storage costs and write bottlenecks of traditional OLTP databases.
- **High Throughput for Any Size:** Whether you are a solo developer hacking on a local agent or an enterprise processing billions of spans, zradar's Rust-based ingestion handles extreme scale with low memory allocation and high concurrency.
- **Zero Vendor Lock-In:** You should own your telemetry. Because zradar speaks standard OpenTelemetry (OTLP), any standard client or SDK can push data to it natively. If you ever want to switch tools, your instrumentation stays exactly the same.
- **Local-First Developer Experience:** Complex telemetry shouldn't require a cloud deployment to test. zradar can run entirely locally with a single container and simple Parquet files, making local debugging and prototyping frictionless.

---

## 🛠️ Development & Contributing

Want to contribute or hack on zradar locally? Here is everything you need.

### 1. Install Prerequisites (manually, once)

- **Rust ≥ 1.93.0** — [rustup.rs](https://rustup.rs)
- **Docker** — for local PostgreSQL
- **Python 3** — for functional test scripts
- **`just`** — `cargo install just`

### 2. First-time setup

```bash
just bootstrap   # installs cargo-nextest, sqlx-cli, and git hooks
just doctor      # verify everything is installed correctly
just dev         # start local Postgres + dev environment
```

### 3. Build & Test Commands

```bash
just fmt              # format
just check            # compile check
just lint             # clippy (zero warnings)
just test             # unit tests
just functional-tests # full end-to-end suite (Docker)
```

**Fast Builds:** `ZRADAR_FAST_BUILD=1 just test` — activates `mold` linker and `sccache` if installed (`sudo apt install mold sccache` / `brew install mold sccache`). Drastically cuts recompile time.

### 4. Configuration

Copy `config.toml.example` to `config.toml` and review connection strings, retention windows, and OTLP ports.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full contribution guide.


