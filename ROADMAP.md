# zradar Roadmap

Our eyes are set on making zradar the observability layer every AI team reaches for — from a single agent on a laptop to a multi-agent Kubernetes cluster. This is where we are headed.

Items marked 🔲 are on the horizon. Items marked 🔄 are in progress. Items marked ✅ are shipped.

---

## Now — Shipped

| Capability | Notes |
|------------|-------|
| ✅ OTLP gRPC ingestion | Traces, metrics, logs via standard OTLP |
| ✅ Parquet-first storage | Local disk or S3-compatible; cost-effective at scale |
| ✅ PostgreSQL control plane | File list, settings, retention, audit — lightweight metadata only |
| ✅ OTel GenAI 1.29 conventions | `gen_ai.*` span attributes mapped and queryable |
| ✅ Agent session tracing | `agent.session_id`, `agent.name`, invocation linking |
| ✅ Multi-tenant isolation | Org + project boundaries enforced at ingest and query |
| ✅ Admin HTTP API | Telemetry queries, analytics, retention, audit |
| ✅ Retention policies | Per-project TTL with automatic compaction |
| ✅ API key auth | Bearer token gRPC + HTTP, revocation |
| ✅ Local dev stack | `just dev` — Docker Compose, no cloud required |
| ✅ 10-framework examples | LangChain, OpenAI Agents SDK, PydanticAI, CrewAI, LlamaIndex, Vercel AI SDK, Anthropic, Google ADK, OpenAI SDK, Mastra |

---

## Near Term

### MCP-Native Observability

Model Context Protocol is becoming the standard for agent–tool communication. Every MCP tool call should be a first-class queryable span — not a generic HTTP trace.

- 🔲 `McpConvention` — maps `mcp.tool.name`, `mcp.server.name`, `mcp.tool.input`, `mcp.tool.output` to queryable fields
- 🔲 `mcp_tool_name`, `mcp_server_name` added to the span model + migration
- 🔲 MCP span type detection in `SpanTypeMapper`
- 🔲 End-to-end example: Claude + MCP server instrumented to zradar

**Why it matters:** Claude users running MCP servers have zero visibility into tool call behavior today. This makes zradar the first tool to give it to them.

---

### Agentic Semantic Conventions

The OTel GenAI SIG is standardizing `gen_ai.agent.*`, `gen_ai.team.*`, and `gen_ai.memory.*` attributes for multi-agent systems.

- 🔲 `gen_ai.agent.id`, `gen_ai.agent.goal`, `gen_ai.agent.status` in `AgentConvention`
- 🔲 `GenAiTeamConvention` — maps `gen_ai.team.*` for multi-agent pipelines
- 🔲 `GenAiMemoryConvention` — maps `gen_ai.memory.type`, `gen_ai.memory.key`
- 🔲 Model fields + migration for new attributes

**Why it matters:** Being the reference implementation of a spec-in-progress means zradar gets cited. Every OTel GenAI SIG participant is a potential contributor.

---

### Simpler Storage Layer

The current plugin architecture (`zradar-plugin-postgres`, `zradar-plugin-s3`) adds complexity without proportional value at this stage. The plan is to collapse storage into a simpler, direct integration.

- 🔲 Remove plugin trait abstraction from storage path
- 🔲 Postgres and S3 wired directly into the service layer
- 🔲 Reduced binary size, simpler dependency graph, easier onboarding for contributors

---

## Medium Term

### Dashboard UI

A visual interface for exploring agent traces, LLM cost trends, tool call analytics, and session timelines — without writing API queries by hand.

- 🔲 Trace timeline view: span tree with LLM and tool annotations
- 🔲 Cost dashboard: token spend by model, project, and time window
- 🔲 Agent session explorer: group spans by `agent.session_id`
- 🔲 MCP tool analytics: latency and error rates by tool and server
- 🔲 Retention and usage settings UI

**Status:** Design in progress. The Admin API is the stable backend; the UI queries it. Contributions welcome — see [UI repo](https://github.com/zvectorlabs/ui).

---

### White Glove Onboarding

First-run experience that walks a developer from zero to seeing their first agent trace in under 5 minutes — for each major framework.

- 🔲 `just onboard <framework>` — guided interactive setup for each SDK
- 🔲 Auto-detects whether zradar is running locally, suggests `just dev` if not
- 🔲 Injects correct OTLP endpoint and API key into the example
- 🔲 Confirms first span arrived and links to the Admin API to explore it
- 🔲 Framework targets: LangChain, OpenAI Agents SDK, Anthropic, Google ADK, Vercel AI SDK

---

### Kubernetes Workflows

Production-grade deployment on Kubernetes with minimal operator burden.

- 🔲 Helm chart: `helm install zradar zvectorlabs/zradar`
- 🔲 Health probes, resource limits, horizontal pod autoscaler template
- 🔲 S3 / GCS / Azure Blob storage backend configuration
- 🔲 Secrets management: external secrets operator integration
- 🔲 Kubernetes deployment example in `examples/kubernetes/`
- 🔲 Guide: running zradar alongside agent workloads in the same cluster

---

### Rust Auto-Instrumentation SDK

A `zradar-instrument` crate that zero-config instruments `async-openai` and Anthropic Rust client calls.

- 🔲 `cargo add zradar-instrument` — adds span wrapping for `async-openai`
- 🔲 Anthropic Rust client instrumentation
- 🔲 Helper macros for agent step tracing built on `tracing`
- 🔲 OTLP gRPC export to `localhost:4317` by default
- 🔲 Published to crates.io as `zradar-instrument`

---

## Longer Horizon

These are directional — no committed timeline yet.

| Item | Notes |
|------|-------|
| Eval / quality scoring | Automatic faithfulness and coherence metrics over ingested spans |
| Alerting | Cost threshold and error rate alerts via webhook |
| Multi-cluster federation | Aggregate traces across multiple zradar deployments |
| WASM plugin host | Safe, sandboxed extension points without Rust compilation |
| Streaming span preview | Real-time span view during agent execution |

---

## What Is Not Planned

To set expectations clearly:

- **Managed cloud service** — zradar is self-hosted. We are not building a SaaS tier.
- **Proprietary query language** — all analytics go through the standard Admin HTTP API or direct Parquet/SQL queries.
- **Agent runtime** — zradar observes agents; it does not run them.

---

## Contributing to the Roadmap

Have a use case not covered here? Open an [RFC issue](https://github.com/zvectorlabs/zradar/issues/new?template=rfc.yml) with your motivation and proposed design. Major roadmap changes go through the RFC process before implementation begins.

Good first issues that directly advance this roadmap are tagged [`good first issue`](https://github.com/zvectorlabs/zradar/issues?q=label%3A%22good+first+issue%22) on GitHub.
